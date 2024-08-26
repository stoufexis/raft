package com.stoufexis.leader.statemachine

import cats.*
import cats.effect.kernel.*
import cats.effect.std.Queue
import cats.effect.std.QueueSink
import cats.effect.std.QueueSource
import cats.implicits.given
import fs2.*
import fs2.concurrent.SignallingRef
import org.typelevel.log4cats.Logger

import com.stoufexis.leader.model.*
import com.stoufexis.leader.rpc.*
import com.stoufexis.leader.service.*
import com.stoufexis.leader.typeclass.IntLike.{*, given}
import com.stoufexis.leader.util.*

import scala.concurrent.duration.FiniteDuration

object Leader:
  class CommitLog[F[_]: Monad, A](
    log:       Log[F, A],
    unmatched: SignallingRef[F, Index],
    matchIdxs: SignallingRef[F, Map[NodeId, Index]]
  ):
    private def commitIdx(nodes: Set[NodeId]): Stream[F, Index] =
      ???

    /** Wait until a majority commits the entry. An empty entries indicates a read request. This still
      * advances the index by appending a special entry to the log
      */
    def appendAndWait(nodes: Set[NodeId], term: Term, entries: Chunk[A]): F[Unit] =
      ???

    def getMatchIndex(node: NodeId): F[Option[Index]] =
      ???

    def setMatchIndex(node: NodeId, idx: Index): F[Unit] =
      ???

    def sendInfo(beginAt: Index): F[(Term, Chunk[A])] =
      ???

    def seekInfo(beginAt: Index): F[Term] =
      ???

    /** If there is no new index for repeatEvery duration, repeat the previous index. When starting up
      * the latest index is sent imediatelly as heartbeat
      */
    def uncommitted(repeatEvery: FiniteDuration): Stream[F, Index] =
      ???

  object CommitLog:
    def apply[F[_]: Temporal, A](log: Log[F, A]): F[CommitLog[F, A]] =
      ???

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
    *
    * TODO: Implement demotion on timeout
    */
  def apply[F[_], A, S: Monoid](
    state:     NodeInfo[S],
    log:       Log[F, A],
    rpc:       RPC[F, A],
    appendsQ:  QueueSource[F, (DeferredSink[F, S], Chunk[A])],
    cfg:       Config,
    automaton: (S, A) => S
  )(using F: Temporal[F], logger: Logger[F]): Stream[F, NodeInfo[S]] =

    val appends: Stream[F, (DeferredSink[F, S], Chunk[A])] =
      Stream.fromQueueUnterminated(appendsQ, 1)

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
    def appender(cl: CommitLog[F, A]): Stream[F, NodeInfo[S]] =
      type SendErr =
        AppendResponse.TermExpired | AppendResponse.IllegalState

      def send(newIdx: Index, node: NodeId): Stream[F, Option[SendErr]] =
        def go(
          matchIdx: Index,
          node:     NodeId,
          seek:     Boolean
        ): Pull[F, Option[Either[SendErr, Index]], Unit] =
          val info: Pull[F, Nothing, (Term, Chunk[A])] = Pull.eval:
            if seek then
              cl.seekInfo(matchIdx).map((_, Chunk.empty))
            else
              cl.sendInfo(matchIdx)

          info.flatMap: (matchIdxTerm, entries) =>
            val request: AppendEntries[A] =
              AppendEntries(
                leaderId     = state.currentNode,
                term         = state.term,
                prevLogIndex = matchIdx,
                prevLogTerm  = matchIdxTerm,
                entries      = entries
              )

            Pull.eval(rpc.appendEntries(node, request)).flatMap:
              case AppendResponse.Accepted if seek =>
                Pull.output1(None) >> go(matchIdx, node, seek = false)
              case AppendResponse.Accepted =>
                Pull.output1(Some(Right(matchIdx + entries.size))) >> Pull.done
              case AppendResponse.NotConsistent       => go(matchIdx - 1, node, seek = true)
              case r @ AppendResponse.TermExpired(_)  => Pull.output1(Some(Left(r))) >> Pull.done
              case r @ AppendResponse.IllegalState(_) => Pull.output1(Some(Left(r))) >> Pull.done
        end go

        for
          matchIdx: Option[Index] <-
            Stream.eval(cl.getMatchIndex(node))

          result: Option[Either[SendErr, Index]] <-
            go(matchIdx.getOrElse(newIdx), node, seek = false).stream

          out: Option[SendErr] <- Stream.eval:
            result match
              case None             => F.pure(None)
              case Some(Left(err))  => F.pure(Some(err))
              case Some(Right(idx)) => cl.setMatchIndex(node, idx) as None
        yield out

      end send

      def node(id: NodeId): Stream[F, (NodeId, Option[SendErr])] =
        cl.uncommitted(cfg.heartbeatEvery)
          .flatMap(send(_, id))
          .map((id, _))

      val outputs: Stream[F, (NodeId, Option[SendErr])] =
        Stream
          .iterable(state.otherNodes)
          .map(node)
          .parJoinUnbounded

      val init: Set[NodeId] =
        Set(state.currentNode)

      outputs.resettableTimeoutAccumulate(
        init      = init,
        timeout   = cfg.staleAfter,
        onTimeout = F.pure(state.transition(Role.Follower))
      ):
        case (nodes, (node, None)) =>
          val newNodes: Set[NodeId] =
            nodes + node

          val info: F[Unit] =
            logger.info("Cluster majority reached, this node is still the leader")

          if nodes.size >= (state.allNodes.size / 2 + 1) then
            info as (init, ResettableTimeout.Reset())
          else
            F.pure(newNodes, ResettableTimeout.Skip())

        case (nodes, (node, Some(AppendResponse.TermExpired(newTerm)))) if state.isNotNew(newTerm) =>
          val warn: F[Unit] = logger.warn:
            s"Got TermExpired with expired term $newTerm from $node"

          warn as (nodes, ResettableTimeout.Skip())

        case (nodes, (node, Some(AppendResponse.TermExpired(newTerm)))) =>
          val warn: F[Unit] = logger.warn:
            s"Current term expired, new term: $newTerm"

          warn as (nodes, ResettableTimeout.Output(state.transition(Role.Follower, newTerm)))

        case (_, (node, Some(AppendResponse.IllegalState(state)))) =>
          F.raiseError(IllegalStateException(s"Node $node detected illegal state: $state"))

    end appender

    def handleAppends(initState: S, cl: CommitLog[F, A]): Stream[F, Nothing] =
      appends.evalScan(initState):
        case (acc, (sink, entries)) =>
          val newState: S =
            entries.foldLeft(acc)(automaton)

          for
            _ <- cl.appendAndWait(state.otherNodes, state.term, entries)
            _ <- sink.complete_(newState)
          yield newState
      .drain

    for
      init: S <-
        log.readAll.fold(Monoid[S].empty)(automaton)

      clog: CommitLog[F, A] <-
        Stream.eval(CommitLog(log))

      out: NodeInfo[S] <-
        Stream(handleIncomingAppends, handleIncomingVotes, handleAppends(init, clog), appender(clog))
          .parJoinUnbounded
    yield out
  end apply
