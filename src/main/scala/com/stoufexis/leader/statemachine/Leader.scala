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
import com.stoufexis.leader.typeclass.IntLike.*
import com.stoufexis.leader.util.*

import scala.concurrent.duration.FiniteDuration

object Leader:
  class CommitLog[F[_]: Monad, A](
    log:       Log[F, A],
    unmatched: SignallingRef[F, Index],
    matchIdxs: SignallingRef[F, Map[NodeId, Index]]
  ):
    /** Wait until a majority commits the entry. An empty entries indicates a read request. This still
      * advances the index by appending a special entry to the log
      */
    def appendAndWait(nodes: Set[NodeId], term: Term, entries: Chunk[A]): F[Unit] =
      ???

    def getMatchIndex(node: NodeId): F[Option[Index]] =
      ???

    def setMatchIndex(node: NodeId, idx: Index): F[Unit] =
      ???

    def sendInfo(beginAt: Index): F[(Term, Index, Chunk[A])] =
      ???

    /** If there is no new index for advertiseEvery duration, repeat the previous index
      */
    def uncommitted[A](f: Index => F[Option[A]]): Stream[F, A] =
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
  def apply[F[_]: Logger, A, S: Monoid](
    state:     NodeInfo[S],
    log:       Log[F, A],
    rpc:       RPC[F, A],
    appendsQ:  QueueSource[F, (DeferredSink[F, S], Chunk[A])],
    cfg:       Config,
    automaton: (S, A) => S
  )(using F: Temporal[F]): Stream[F, NodeInfo[S]] =

    val appends: Stream[F, (Option[DeferredSink[F, S]], Chunk[A])] =
      Stream
        .fromQueueUnterminated(appendsQ, 1)
        .map((sink, chunk) => (Some(sink), chunk))
        // Heartbeat if the required time has passed since the last time an append was made manually
        .timeoutOnPullTo(cfg.heartbeatEvery, Stream((None, Chunk.empty)))

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
      */
    def appender(st: CommitLog[F, A]): List[Stream[F, NodeInfo[S]]] =
      def send(nextIdx: Index, node: NodeId): F[Option[NodeInfo[S]]] =
        def go(matchIdx: Index, node: NodeId, seek: Boolean): F[Either[NodeInfo[S], Index]] =
          st.sendInfo(matchIdx).flatMap: (matchIdxTerm, commitIdx, entries) =>
            val request: AppendEntries[A] =
              AppendEntries(
                leaderId     = state.currentNode,
                term         = state.term,
                prevLogIndex = matchIdx,
                prevLogTerm  = matchIdxTerm,
                leaderCommit = commitIdx,
                entries      = if seek then Chunk.empty else entries
              )

            rpc.appendEntries(node, request).flatMap:
              case AppendResponse.Accepted if seek     => go(matchIdx, node, seek = false)
              case AppendResponse.Accepted             => F.pure(Right(matchIdx + entries.size))
              case AppendResponse.NotConsistent        => go(matchIdx - 1, node, seek = true)
              case AppendResponse.TermExpired(newTerm) => F.pure(Left(state.toFollower(newTerm)))
              case AppendResponse.IllegalState(state)  => IllegalStateException(state).raiseError
        end go

        for
          matchIdx: Option[Index] <-
            st.getMatchIndex(node)

          result: Either[NodeInfo[S], Index] <-
            go(matchIdx.getOrElse(nextIdx), node, seek = false)

          out: Option[NodeInfo[S]] <- result match
            case Left(newState)     => F.pure(Some(newState))
            case Right(newMatchIdx) => st.setMatchIndex(node, newMatchIdx) as None
        yield out

      end send

      state.otherNodes.toList.map: node =>
        st.uncommitted(send(_, node))

    end appender

    def handleAppends(initState: S, st: CommitLog[F, A]): Stream[F, Nothing] =
      appends.evalScan(initState):
        case (acc, (sink, entries)) =>
          val newState: S =
            entries.foldLeft(acc)(automaton)

          for
            _ <- st.appendAndWait(state.otherNodes, state.term, entries)
            _ <- sink.fold(F.unit)(_.complete_(newState))
          yield newState
      .drain

    for
      init: S <-
        log.readAll.fold(Monoid[S].empty)(automaton)

      clog: CommitLog[F, A] <-
        Stream.eval(CommitLog(log))

      out: NodeInfo[S] <- raceFirst:
        handleIncomingAppends :: handleIncomingVotes :: handleAppends(init, clog) :: appender(clog)
    yield out
  end apply
