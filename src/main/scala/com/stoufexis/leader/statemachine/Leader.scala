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
import com.stoufexis.leader.typeclass.Counter.*
import com.stoufexis.leader.util.*

import scala.concurrent.duration.FiniteDuration

object Leader:
  class LocalLog[F[_], A](log: Log[F, A]):
    /** Wait until a majority commits the entry
      */
    def appendAndWait(term: Term, entry: A): F[Unit] = ???

    def sendInfo(index: Index): F[(Term, Index, Chunk[A])] = ???

    def commit(node: NodeId, idx: Index): F[Unit] = ???

    /** If there is no new index for advertiseEvery duration, repeat the previous index
      */
    def uncommitted(node: NodeId, advertiseEvery: FiniteDuration): Stream[F, Index] = ???

    def majorityCommit(idx: Index): F[Unit] = ???

  def apply[F[_]: Logger, A, S](
    state:     NodeInfo[S],
    log:       Log[F, A],
    rpc:       RPC[F, A],
    appends:   QueueSource[F, (A, DeferredSink[F, S])],
    cfg:       Config,
    automaton: (S, A) => F[S]
  )(using F: Temporal[F]): Stream[F, NodeInfo[S]] =
    import com.stoufexis.leader.util.ResettableTimeout.*

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

    def appender(localLog: LocalLog[F, A]): List[Stream[F, NodeInfo[S]]] =
      def send(
        matchIdx: Index,
        node:     NodeId,
        seek:     Boolean = false
      ): F[Either[NodeInfo[S], Index]] =
        val response: F[(AppendResponse, Index)] =
          for
            (matchIdxTerm, commitIdx, entries) <-
              localLog.sendInfo(matchIdx)

            request: AppendEntries[A] =
              AppendEntries(
                leaderId     = state.currentNode,
                term         = state.term,
                prevLogIndex = matchIdx,
                prevLogTerm  = matchIdxTerm,
                leaderCommit = commitIdx,
                entries      = if seek then Chunk.empty else entries
              )

            response: AppendResponse <-
              rpc.appendEntries(node, request)
          yield (response, matchIdx.increaseBy(entries.size))

        response.flatMap:
          case (AppendResponse.Accepted, newMatchIdx) if seek =>
            send(matchIdx, node, seek = false)
          case (AppendResponse.Accepted, newMatchIdx) =>
            localLog.commit(node, newMatchIdx) as (Right(newMatchIdx))
          case (AppendResponse.NotConsistent, _) =>
            send(matchIdx.previous, node, seek = true)
          case (AppendResponse.TermExpired(newTerm), _) =>
            F.pure(Left(state.transition(Role.Follower, newTerm)))
          case (AppendResponse.IllegalState(state), _) =>
            F.raiseError(IllegalStateException(state))

      /** Maintains a matchIndex for each node, which points to the last index in the log for which
        * every preceeding entry matches, including the matchIndex entry.
        *
        * If there is a new uncommitted index, it attempts to send the uncommitted records to the
        * node. If the node returns a NotConsistent, then attempt to find the largest index for
        * which the logs match, by decreasing the matchIndex by 1 and attempting to send an empty
        * AppendEntries.
        *
        * If there is no new uncommitted index and heartbeatEvery time has passed, will emit
        * heartbeat, which is a seek request, ie an append entries with no entries. The heartbeat
        * may also fail the consistency check, in which case a seek is attempted to find the correct
        * matchIndex, and then a send is attempted.
        *
        * If we encounter a TermExpired, emit a new state, which signals the upstream for this
        * stream's termination. In all other cases nothing is emitted.
        */
      def forNode(node: NodeId): Stream[F, NodeInfo[S]] =
        localLog
          .uncommitted(node, cfg.heartbeatEvery)
          .evalMapFilterAccumulate(Option.empty[Index]): (matchIdx, nextIdx) =>
            send(matchIdx.getOrElse(nextIdx), node).map:
              case Left(err)          => (matchIdx, Some(err))
              case Right(newMatchIdx) => (Some(newMatchIdx), None)

      state.otherNodes.map(forNode).toList
    end appender

    def handleAppends(localLog: LocalLog[F, A]): Stream[F, Nothing] =
      Stream
        .fromQueueUnterminated(appends)
        .evalScan(state.automatonState):
          case (acc, (newEntry, sink)) =>
            for
              _    <- localLog.appendAndWait(state.term, newEntry)
              newS <- automaton(acc, newEntry)
              _    <- sink.complete_(newS)
            yield newS
        .drain

    // handleIncomingAppends merge handleIncomingVotes merge handleAppends
    ???
  end apply