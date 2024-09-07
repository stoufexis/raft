package com.stoufexis.raft.statemachine

import cats.*
import cats.effect.kernel.*
import cats.implicits.given
import fs2.*
import org.typelevel.log4cats.Logger

import com.stoufexis.raft.model.*
import com.stoufexis.raft.rpc.*
import com.stoufexis.raft.typeclass.IntLike.*

import scala.concurrent.duration.FiniteDuration

object Follower:
  def apply[F[_], A, S: Monoid](
    state: NodeInfo[S]
  )(using
    F:       Temporal[F],
    log:     Log[F, A],
    rpc:     RPC[F, A, S],
    timeout: Timeout[F],
    logger:  Logger[F]
  ): Stream[F, NodeInfo[S]] =
    for
      electionTimeout: FiniteDuration <-
        Stream.eval(timeout.nextElectionTimeout)

      (_, initIdx: Index) <-
        Stream.eval(log.lastTermIndex)

      out: NodeInfo[S] <-
        raceFirst(
          handleIncomingAppendsAndVotes(state, electionTimeout, initIdx),
          handleClientRequests(state)
        )
    yield out

  def handleClientRequests[F[_], A, S](state: NodeInfo[S])(using rpc: RPC[F, A, S]): Stream[F, Nothing] =
    rpc
      .incomingClientRequests
      .evalMap(_.sink.complete(ClientResponse.knownLeader(state.knownLeader)))
      .drain

  /** Appends and votes are unified since the vote state machine needs to know the current index of the log.
    * Keeping it in the local loop is more efficient that always reading it from the log.
    */
  def handleIncomingAppendsAndVotes[F[_], A, S](
    state:           NodeInfo[S],
    electionTimeout: FiniteDuration,
    initIdx:         Index
  )(using
    F:      Temporal[F],
    rpc:    RPC[F, A, S],
    log:    Log[F, A],
    logger: Logger[F]
  ): Stream[F, NodeInfo[S]] =

    /** We can use the state.term, since we will always be writting using this term. If a new term is
      * observed, we transition to a follower of the next term.
      */
    def isUpToDate(currentIndex: Index, req: RequestVote): Boolean =
      req.lastLogTerm > state.term || (req.lastLogTerm == state.term && req.lastLogIndex >= currentIndex)

    (rpc.incomingAppends mergeHaltBoth rpc.incomingVotes).resettableTimeoutAccumulate(
      init      = (Option.empty[NodeId], initIdx),
      onTimeout = F.pure(state.toCandidateNextTerm),
      timeout   = electionTimeout
    ):
      // APPENDS
      case (st, IncomingAppend(req, sink)) if state.isExpired(req.term) =>
        (req.termExpired(state, sink) as st, ResettableTimeout.Skip())

      /** Append from current known leader. Fulfill it. If the leader has a larger term, or if we previously
        * did not know who the leader was, transition, so that other state machines will know about the
        * leader.
        */
      case ((vf, i), IncomingAppend(req, sink)) if state.isCurrentLeader(req.term, req.leaderId) =>
        val append: F[Index] =
          log.appendChunkIfMatches(req.prevLogTerm, req.prevLogIndex, req.term, req.entries).flatMap:
            case None       => req.inconsistent(sink) as i
            case Some(newI) => req.accepted(sink) as newI

        (append.tupleLeft(vf), ResettableTimeout.Reset())

      /** Leader from new term. Let the request be fulfilled after we transition
        */
      case (st, IncomingAppend(req, sink)) =>
        (F.pure(st), ResettableTimeout.Output(state.toFollower(req.term, req.leaderId)))

      // VOTES
      case (st, IncomingVote(req, sink)) if state.isExpired(req.term) =>
        (req.termExpired(state, sink) as st, ResettableTimeout.Skip())

      /** This candidate has our vote already, grant again.
        */
      case (s @ (Some(f), i), IncomingVote(req, sink)) if state.isCurrent(req.term) && f == req.candidateId =>
        (req.grant(sink) as s, ResettableTimeout.Skip())

      /** We voted for someone else already.
        */
      case (st @ (Some(vf), i), IncomingVote(req, sink)) if state.isCurrent(req.term) =>
        (req.reject(sink) as st, ResettableTimeout.Skip())

      /** This candidate requested a vote first and its log is at least as advanced as ours. Grant the vote.
        */
      case ((None, i), IncomingVote(req, sink)) if state.isCurrent(req.term) && isUpToDate(i, req) =>
        (req.grant(sink) as (Some(req.candidateId), i), ResettableTimeout.Skip())

      /** This candidate requested a vote first but its log is not at least as advanced as ours. Reject the
        * vote.
        */
      case ((None, i), IncomingVote(req, sink)) if state.isCurrent(req.term) =>
        (req.reject(sink) as (None, i), ResettableTimeout.Skip())

      /** Election for new term. Vote after the transition.
        */
      case (vf, IncomingVote(req, sink)) =>
        (F.pure(vf), ResettableTimeout.Output(state.toFollowerUnknownLeader(req.term)))
