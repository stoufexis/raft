package com.stoufexis.raft.statemachine

import cats.*
import cats.data.NonEmptySeq
import cats.effect.kernel.*
import cats.implicits.given
import fs2.*
import org.typelevel.log4cats.Logger

import com.stoufexis.raft.model.*
import com.stoufexis.raft.persist.Log
import com.stoufexis.raft.rpc.*
import com.stoufexis.raft.typeclass.IntLike.*
import scala.concurrent.duration.FiniteDuration

object Follower:
  def apply[F[_]: Temporal: Logger, In, S](state: NodeInfo)(using
    cfg: Config[F, In, ?, S]
  ): Behaviors[F] =
    Behaviors(
      cfg.inputs.incomingClientRequests.respondWithLeader(state.knownLeader),
      appendsAndVotes(state)
    )

  /** Appends and votes are unified since the vote state machine needs to know the current index of the log.
    * Keeping it in the local loop is more efficient that always reading it from the log.
    */
  def appendsAndVotes[F[_], In, S](state: NodeInfo)(using
    F:       Temporal[F],
    logger:  Logger[F],
    log:     Log[F, In],
    timeout: ElectionTimeout[F],
    inputs:  InputSource[F, In, ?, S]
  ): Stream[F, NodeInfo] =
    /** We can use the state.term, since we will always be writting using this term. If a new term is
      * observed, we transition to a follower of the next term.
      */
    def isUpToDate(lastLogTerm: Term, lastLogIndex: Index, req: RequestVote): Boolean =
      req.lastLogTerm > lastLogTerm || (req.lastLogTerm == lastLogTerm && req.lastLogIndex >= lastLogIndex)

    for
      electionTimeout: FiniteDuration <-
        Stream.eval(timeout.nextElectionTimeout)

      inits: (Term, Index) <-
        Stream.eval(log.lastTermIndex.map(_.getOrElse(Term.uninitiated, Index.uninitiated)))

      out: NodeInfo <-
        (inputs.incomingAppends mergeHaltBoth inputs.incomingVotes).resettableTimeoutAccumulate(
          init      = inits,
          onTimeout = F.pure(state.toCandidateNextTerm),
          timeout   = electionTimeout
        ):
          // APPENDS
          case (st, IncomingAppend(req, sink)) if state.isExpired(req.term) =>
            ResettableTimeout.Skip(req.termExpired(state, sink) as st)

          /** Append from current known leader. Fulfill it. If the leader has a larger term, or if we
            * previously did not know who the leader was, transition, so that other state machines will know
            * about the leader.
            */
          case (st @ (t, i), IncomingAppend(req, sink)) if state.isCurrentLeader(req.term, req.leaderId) =>
            val matches: F[Boolean] =
              if req.prevLogIndex <= Index.uninitiated then
                F.pure(true)
              else
                log.matches(req.prevLogTerm, req.prevLogIndex)

            ResettableTimeout.Reset:
              NonEmptySeq.fromSeq(req.entries) match
                case None =>
                  matches.ifM(req.accepted(sink), req.inconsistent(sink)) as st

                case Some(entries) =>
                  val onMatches: F[(Term, Index)] =
                    for
                      _ <- log.deleteAfter(req.prevLogIndex)
                      i <- log.append(req.term, entries)
                      _ <- req.accepted(sink)
                    yield (req.term, i)

                  val onNotMatches: F[(Term, Index)] =
                    req.inconsistent(sink) as st

                  matches.ifM(onMatches, onNotMatches)

          case (_, IncomingAppend(req, sink)) =>
            ResettableTimeout.outputPure(state.toFollower(req.term, req.leaderId))

          // VOTES
          case (st, IncomingVote(req, sink)) if state.isExpired(req.term) =>
            ResettableTimeout.Skip(req.termExpired(state, sink) as st)

          /** This candidate has our vote already, grant again.
            */
          case (st, IncomingVote(req, sink)) if state.isCurrentVotee(req.term, req.candidateId) =>
            ResettableTimeout.Skip(req.grant(sink) as st)

          /** We voted for someone else already.
            */
          case (st, IncomingVote(req, sink)) if state.isCurrent(req.term) && state.hasVoted =>
            ResettableTimeout.Skip(req.reject(sink) as st)

          /** This candidate requested a vote first and its log is at least as advanced as ours. Grant the
            * vote.
            */
          case ((t, i), IncomingVote(req, sink)) if state.isCurrent(req.term) && isUpToDate(t, i, req) =>
            ResettableTimeout.outputPure(state.toVotedFollower(req.candidateId))

          /** This candidate requested a vote first but its log is not at least as advanced as ours. Reject
            * the vote.
            */
          case (st, IncomingVote(req, sink)) if state.isCurrent(req.term) =>
            ResettableTimeout.Skip(req.reject(sink) as st)

          /** Election for new term. Vote after the transition.
            */
          case (_, IncomingVote(req, sink)) =>
            ResettableTimeout.outputPure(state.toFollowerUnknownLeader(req.term))
    yield out
