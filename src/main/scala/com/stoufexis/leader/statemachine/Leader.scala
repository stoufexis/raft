package com.stoufexis.leader.statemachine

import cats.*
import cats.effect.implicits.given
import cats.effect.kernel.*
import cats.effect.kernel.Resource.ExitCase
import cats.effect.std.Supervisor
import cats.implicits.given
import fs2.*
import org.typelevel.log4cats.Logger

import com.stoufexis.leader.model.*
import com.stoufexis.leader.rpc.*
import com.stoufexis.leader.typeclass.IntLike.{*, given}
import com.stoufexis.leader.util.*

trait Leader[F[_], A, S]:
  def await: F[Outcome[F, Throwable, NodeInfo[S]]]

  def write(a: Chunk[A]): F[Option[S]]

  def read: F[Option[S]]

object Leader:

  case class WaitingClient[F[_]: Monad, S](
    idxStart: Index,
    idxEnd:   Index,
    sink:     DeferredSink[F, Option[S]]
  )

  /** Linearizability of reads is implemented quite inefficiently right now. A read is inserted as a
    * special entry in the log, and the read returns to the client only after the special entry is marked
    * as committed. Section "6.4 Processing read-only queries more efficiently" of the [Raft
    * paper](https://github.com/ongardie/dissertation#readme) explains a more efficient implementation.
    * Implementing the efficient algorithm is left as a future effort.
    *
    * This implementation of raft only updates the state machine in the leader node. Other nodes simply
    * replicate the log. The state machine in other nodes is reconstructed from the log if they become
    * the leader. TODO: I think this means I can get rid of the leaderCommit in AppendEntries requests
    *
    * @param state
    * @param log
    * @param rpc
    * @param appendsQ
    *   Request for appending to the state machine. An empty chunk indicates a read request.
    * @param cfg
    * @param automaton
    * @param F
    * @return
    */
  def apply[F[_], A, S: Monoid](
    state:     NodeInfo[S],
    log:       Log[F, A],
    rpc:       RPC[F, A],
    cfg:       Config,
    automaton: (S, A) => S
  )(using F: Temporal[F], logger: Logger[F]): Resource[F, Leader[F, A, S]] =

    val handleIncomingAppends: Stream[F, NodeInfo[S]] =
      rpc.incomingAppends.evalMapFilter:
        case IncomingAppend(req, sink) if state.isExpired(req.term) =>
          req.termExpired(state, sink) as None

        case IncomingAppend(req, sink) if state.isCurrent(req.term) =>
          req.duplicateLeaders(sink)

        // newer leader detected
        case IncomingAppend(req, sink) =>
          req.accepted(sink) as Some(state.transition(Role.Follower, req.term))

    val handleIncomingVotes: Stream[F, NodeInfo[S]] =
      rpc.incomingVotes.evalMapFilter:
        case IncomingVote(req, sink) if state.isExpired(req.term) =>
          req.termExpired(state, sink) as None

        case IncomingVote(req, sink) if state.isCurrent(req.term) =>
          req.reject(sink) as None

        // new election detected
        case IncomingVote(req, sink) =>
          req.grant(sink) as Some(state.transition(Role.VotedFollower, req.term))

    /** Maintains a matchIndex for each node, which points to the last index in the log for which every
      * preceeding entry matches, including the matchIndex entry.
      *
      * If there is a new uncommitted index, it attempts to send the uncommitted records to the node. If
      * the node returns a NotConsistent, then attempt to find the largest index for which the logs
      * match, by decreasing the matchIndex by 1 and attempting to send an empty AppendEntries.
      *
      * If there is no new uncommitted index and heartbeatEvery time has passed, will emit heartbeat,
      * which is a seek request, ie an append entries with no entries. The heartbeat may also fail the
      * consistency check, in which case a seek is attempted to find the correct matchIndex, and then a
      * send is attempted.
      *
      * If we encounter a TermExpired, emit a new state, which signals the upstream for this stream's
      * termination. In all other cases nothing is emitted.
      *
      * If the majority of the cluster is not reached with any request for staleAfter time, demote self
      * to follower. This makes sure that a partitioned leader will quickly stop acting as a leader, even
      * if it does not notice another leader with a higher term.
      */

    def appender(
      node:      NodeId,
      newIdxs:   CloseableTopic[F, Index],
      matchIdxs: CloseableTopic[F, (NodeId, Index)]
    ): Stream[F, NodeInfo[S]] =
      def send(matchIdx: Index, newIdx: Index): F[Either[NodeInfo[S], Index]] =
        // Should be called whenever the node successfully responded, even if AppendEntries ultimately failed.
        // It keeps the partitionChecker from timing out.
        // Until we figure out the new matchIdx, re-send the previously valid matchIdx
        def pinged(i: Index = matchIdx): F[Unit] =
          matchIdxs.publish((node, i)).void

        def go(matchIdx: Index, newIdx: Index, seek: Boolean = false): F[Either[NodeInfo[S], Index]] =
          val info: F[(Term, Chunk[A])] =
            if seek then
              log.term(matchIdx).map((_, Chunk.empty))
            else
              log.range(matchIdx, newIdx)

          info.flatMap: (matchIdxTerm, entries) =>
            val request: AppendEntries[A] =
              AppendEntries(
                leaderId     = state.currentNode,
                term         = state.term,
                prevLogIndex = matchIdx,
                prevLogTerm  = matchIdxTerm,
                entries      = entries
              )

            rpc.appendEntries(node, request).flatMap:
              case AppendResponse.Accepted if seek =>
                pinged() >> go(matchIdx, newIdx, seek = false)

              case AppendResponse.Accepted =>
                pinged(newIdx) as Right(newIdx)

              case AppendResponse.NotConsistent =>
                pinged() >> go(matchIdx - 1, newIdx, seek = true)

              case AppendResponse.TermExpired(t) =>
                pinged() as Left(state.toFollower(t))

              case AppendResponse.IllegalState(msg) =>
                F.raiseError(IllegalStateException(msg))
        end go

        go(matchIdx, newIdx, seek = false)
      end send

      val latest: Stream[F, Index] =
        Stream.eval(log.current)

      val newIdxOrHeartbeat: Stream[F, Index] =
        (latest ++ newIdxs.subscribe)
          .timeoutOnPullTo(cfg.heartbeatEvery, latest)

      newIdxOrHeartbeat
        .evalMapFilterAccumulate(Option.empty[Index]): (matchIdx, newIdx) =>
          for
            result: Either[NodeInfo[S], Index] <-
              send(matchIdx.getOrElse(newIdx), newIdx)

            newMatchIdx: Option[Index] =
              result.toOption <+> matchIdx

            output: Option[NodeInfo[S]] =
              result.left.toOption
          yield (newMatchIdx, output)

    end appender

    def partitionChecker(matchIdxs: CloseableTopic[F, (NodeId, Index)]): Stream[F, NodeInfo[S]] =
      matchIdxs.subscribe.resettableTimeoutAccumulate(
        init      = Set(state.currentNode),
        timeout   = cfg.staleAfter,
        onTimeout = F.pure(state.toFollower)
      ):
        case (nodes, (node, _)) =>
          val newNodes: Set[NodeId] =
            nodes + node

          val info: F[Unit] =
            logger.info("Cluster majority reached, this node is still the leader")

          val debug: F[Unit] =
            logger.debug(s"Received response from $node")

          if state.isMajority(newNodes) then
            info as (Set(state.currentNode), ResettableTimeout.Reset())
          else
            debug as (newNodes, ResettableTimeout.Skip())

    end partitionChecker

    def commitIdx(matchIdx: CloseableTopic[F, (NodeId, Index)]): Stream[F, Index] =
      matchIdx
        .subscribe
        .scan(Map.empty[NodeId, Index])(_ + _)
        .mapFilter(Pure.commitIdxFromMatch(state.otherNodes, _))
        .discrete
        .evalTap(cidx => logger.debug(s"Commit index is now at $cidx"))

    def stateMachine(
      appends:  CloseableTopic[F, (DeferredSink[F, Option[S]], Chunk[A])],
      matchIdx: CloseableTopic[F, (NodeId, Index)],
      newIdxs:  CloseableTopic[F, Index],
      initS:    S
    ): Stream[F, Nothing] =
      // Assumes that matchIdxs for each node always increase
      val commitsAndAppends: Stream[F, Either[Index, (DeferredSink[F, Option[S]], Chunk[A])]] =
        commitIdx(matchIdx).mergeEither(appends.subscribe)

      val acc: (Option[WaitingClient[F, S]], S) =
        (None, initS)

      commitsAndAppends.evalScanDrain(acc):
        case ((Some(client), s), Left(commitIdx)) =>
          for
            (_, entries) <- log.range(client.idxStart, client.idxEnd)
            newS         <- F.pure(entries.foldLeft(s)(automaton))
            _            <- client.sink.complete_(Some(newS))
          yield (None, newS)

        case (st @ (None, _), Left(_)) =>
          F.pure(st)

        case (st @ (Some(_), _), Right((sink, _))) =>
          sink.complete(None) as st

        case ((None, s), Right((sink, entries))) =>
          for
            (start, end) <- log.appendChunk(state.term, entries)
            _            <- newIdxs.publish(end)
          yield (Some(WaitingClient(start, end, sink)), s)

    end stateMachine

    def streams(
      newIdxs:   CloseableTopic[F, Index],
      matchIdxs: CloseableTopic[F, (NodeId, Index)],
      appends:   CloseableTopic[F, (DeferredSink[F, Option[S]], Chunk[A])]
    ): Stream[F, NodeInfo[S]] =
      for
        initState: S <-
          log.readAll.fold(Monoid[S].empty)(automaton)

        checker: Stream[F, NodeInfo[S]] =
          partitionChecker(matchIdxs)

        client: Stream[F, Nothing] =
          stateMachine(appends, matchIdxs, newIdxs, initState)

        appenders: List[Stream[F, NodeInfo[S]]] =
          state
            .allNodes
            .toList
            .map(appender(_, newIdxs, matchIdxs))

        streams: List[Stream[F, NodeInfo[S]]] =
          handleIncomingAppends :: handleIncomingVotes :: checker :: client :: appenders

        out: NodeInfo[S] <-
          Stream
            .iterable(streams)
            .parJoinUnbounded
      yield out

    // Closes the topics after a single NodeInfo[S] is produced
    // Closing the topics interrupts subscribers and makes publishes no-ops
    // Any further writes or reads will return None
    for
      sup: Supervisor[F] <-
        Supervisor[F](await = false)

      cidx: CloseableTopic[F, Index] <-
        Resource.eval(CloseableTopic[F, Index])

      midx: CloseableTopic[F, (NodeId, Index)] <-
        Resource.eval(CloseableTopic[F, (NodeId, Index)])

      appends: CloseableTopic[F, (DeferredSink[F, Option[S]], Chunk[A])] <-
        Resource.eval(CloseableTopic[F, (DeferredSink[F, Option[S]], Chunk[A])])

      run: F[NodeInfo[S]] =
        streams(cidx, midx, appends)
          .take(1)
          .compile
          .lastOrError

      f: Fiber[F, Throwable, NodeInfo[S]] <-
        Resource.eval(sup.supervise(run))

      _ <-
        Resource.onFinalize(cidx.close >> midx.close >> appends.close.void)
    yield new Leader[F, A, S]:
      def await: F[Outcome[F, Throwable, NodeInfo[S]]] =
        f.join

      // Gets cancelled if streams terminate
      def writeFull(as: Chunk[A]): F[Either[Outcome[F, Throwable, NodeInfo[S]], Option[S]]] =
        f.join.race:
          for
            deferred <- Deferred[F, Option[S]]
            s        <- appends.publish(deferred, as).ifM(deferred.get, F.pure(None))
          yield s

      def write(as: Chunk[A]): F[Option[S]] =
        writeFull(as).map(_.toOption.flatten)

      def read: F[Option[S]] =
        write(Chunk.empty)

  end apply
