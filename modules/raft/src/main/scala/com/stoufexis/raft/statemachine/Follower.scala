package com.stoufexis.raft.statemachine

import cats.*
import cats.effect.kernel.*
import cats.implicits.given
import fs2.*
import org.typelevel.log4cats.Logger

import com.stoufexis.raft.persist.Log
import com.stoufexis.raft.rpc.*
import com.stoufexis.raft.typeclass.IntLike.*
import com.stoufexis.raft.model.*

object Follower:
  def apply[F[_]: Temporal: Logger, A, S: Monoid](state: NodeInfo)(using cfg: Config[F, A, S]): Behaviors[F] =
    Behaviors(
      cfg.inputs.incomingClientRequests.respondWithLeader(state.knownLeader),
      appendsAndVotes(state)
    )

  /** Appends and votes are unified since the vote state machine needs to know the current index of the log.
    * Keeping it in the local loop is more efficient that always reading it from the log.
    */
  def appendsAndVotes[F[_], A, S](state: NodeInfo)(using
    F:       Temporal[F],
    logger:  Logger[F],
    log:     Log[F, A],
    timeout: ElectionTimeout[F],
    inputs:  InputSource[F, A, S]
  ): Stream[F, NodeInfo] =
    /** We can use the state.term, since we will always be writting using this term. If a new term is
      * observed, we transition to a follower of the next term.
      */
    def isUpToDate(currentIndex: Index, req: RequestVote): Boolean =
      req.lastLogTerm > state.term || (req.lastLogTerm == state.term && req.lastLogIndex >= currentIndex)

    for
      electionTimeout <- Stream.eval(timeout.nextElectionTimeout)
      (_, initIdx)    <- Stream.eval(log.lastTermIndex)

      out: NodeInfo <-
        (inputs.incomingAppends mergeHaltBoth inputs.incomingVotes).resettableTimeoutAccumulate(
          init      = initIdx,
          onTimeout = F.pure(state.toCandidateNextTerm),
          timeout   = electionTimeout
        ):
          // APPENDS
          case (i, IncomingAppend(req, sink)) if state.isExpired(req.term) =>
            ResettableTimeout.Skip(req.termExpired(state, sink) as i)

          /** Append from current known leader. Fulfill it. If the leader has a larger term, or if we
            * previously did not know who the leader was, transition, so that other state machines will know
            * about the leader.
            */
          case (i, IncomingAppend(req, sink)) if state.isCurrentLeader(req.term, req.leaderId) =>
            val append: F[Index] =
              log.overwriteChunkIfMatches(req.prevLogTerm, req.prevLogIndex, req.term, req.entries)
                .flatMap:
                  case None       => req.inconsistent(sink) as i
                  case Some(newI) => req.accepted(sink) as newI

            ResettableTimeout.Reset(append)

          case (_, IncomingAppend(req, sink)) =>
            ResettableTimeout.outputPure(state.toFollower(req.term, req.leaderId))

          // VOTES
          case (i, IncomingVote(req, sink)) if state.isExpired(req.term) =>
            ResettableTimeout.Skip(req.termExpired(state, sink) as i)

          /** This candidate has our vote already, grant again.
            */
          case (i, IncomingVote(req, sink)) if state.isCurrentVotee(req.term, req.candidateId) =>
            ResettableTimeout.Skip(req.grant(sink) as i)

          /** We voted for someone else already.
            */
          case (i, IncomingVote(req, sink)) if state.isCurrent(req.term) && state.hasVoted =>
            ResettableTimeout.Skip(req.reject(sink) as i)

          /** This candidate requested a vote first and its log is at least as advanced as ours. Grant the
            * vote.
            */
          case (i, IncomingVote(req, sink)) if state.isCurrent(req.term) && isUpToDate(i, req) =>
            ResettableTimeout.outputPure(state.toVotedFollower(req.candidateId))

          /** This candidate requested a vote first but its log is not at least as advanced as ours. Reject
            * the vote.
            */
          case (i, IncomingVote(req, sink)) if state.isCurrent(req.term) =>
            ResettableTimeout.Skip(req.reject(sink) as i)

          /** Election for new term. Vote after the transition.
            */
          case (_, IncomingVote(req, sink)) =>
            ResettableTimeout.outputPure(state.toFollowerUnknownLeader(req.term))
    yield out
