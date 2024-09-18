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
  def apply[F[_], In, S](state: NodeInfo)(using
    F:       Temporal[F],
    logger:  Logger[F],
    log:     Log[F, In],
    timeout: ElectionTimeout[F],
    inputs:  InputSource[F, In, ?, S]
  ): Behaviors[F] =
    Behaviors(
      inputs.incomingClientRequests.respondWithLeader(state.knownLeader),
      appends(state),
      votes(state)
    )

  def isUpToDate(lastLogTerm: Term, lastLogIndex: Index, req: RequestVote): Boolean =
    req.lastLogTerm > lastLogTerm || (req.lastLogTerm == lastLogTerm && req.lastLogIndex >= lastLogIndex)

  def votes[F[_], In, S](state: NodeInfo)(using
    F:      Temporal[F],
    logger: Logger[F],
    log:    Log[F, In],
    inputs: InputSource[F, In, ?, S]
  ): Stream[F, NodeInfo] =
    if state.hasVoted then
      inputs.incomingVotes.evalMapFirstSome:
        case IncomingVote(req, sink) if state.isExpired(req.term) =>
          req.termExpired(state, sink) as None

        case IncomingVote(req, sink) if state.isCurrent(req.term) && state.isVotee(req.candidateId) =>
          req.grant(sink) as None

        case IncomingVote(req, sink) if state.isCurrent(req.term) =>
          req.reject(sink) as None

        case IncomingVote(req, sink) =>
          F.pure(Some(state.toFollowerUnknownLeader(req.term)))
    else
      for
        (lTerm, lIndex) <-
          Stream.eval(log.lastTermIndex.map(_.getOrElse(Term.uninitiated, Index.uninitiated)))

        out: NodeInfo <- inputs.incomingVotes.evalMapFirstSome:
          case IncomingVote(req, sink) if state.isExpired(req.term) =>
            req.termExpired(state, sink) as None

          case IncomingVote(req, sink) if state.isCurrent(req.term) && isUpToDate(lTerm, lIndex, req) =>
            F.pure(Some(state.toVotedFollower(req.candidateId)))

          case IncomingVote(req, sink) if state.isCurrent(req.term) =>
            req.reject(sink) as None

          case IncomingVote(req, sink) =>
            F.pure(Some(state.toFollowerUnknownLeader(req.term)))
      yield out

  def appends[F[_], In, S](state: NodeInfo)(using
    F:       Temporal[F],
    logger:  Logger[F],
    log:     Log[F, In],
    timeout: ElectionTimeout[F],
    inputs:  InputSource[F, In, ?, S]
  ): Stream[F, NodeInfo] =
    for
      electionTimeout: FiniteDuration <-
        Stream.eval(timeout.nextElectionTimeout)

      out: NodeInfo <-
        inputs.incomingAppends.resettableTimeoutAccumulate(
          init      = (),
          onTimeout = F.pure(state.toCandidateNextTerm),
          timeout   = electionTimeout
        ):
          // APPENDS
          case ((), IncomingAppend(req, sink)) if state.isExpired(req.term) =>
            ResettableTimeout.Skip(req.termExpired(state, sink))

          /** Append from current known leader. Fulfill it. If the leader has a larger term, or if we
            * previously did not know who the leader was, transition, so that other state machines will know
            * about the leader.
            */
          case ((), IncomingAppend(req, sink)) if state.isCurrentLeader(req.term, req.leaderId) =>
            val matches: F[Boolean] =
              if req.prevLogIndex <= Index.uninitiated then
                F.pure(true)
              else
                log.matches(req.prevLogTerm, req.prevLogIndex)

            ResettableTimeout.Reset:
              NonEmptySeq.fromSeq(req.entries) match
                case None =>
                  matches.ifM(req.accepted(sink), req.inconsistent(sink))

                case Some(entries) =>
                  // respond before appending to make the leader's heartbeat timeout reset
                  // We may crash after responding but before appending.
                  // In that case we will respond with Inconsistent on the next append from the leader, and the leader will then send us the record we missed because of the crash
                  // We are also safe because of the same mechanism im case the delete succeeds and the append does not
                  val onMatches: F[Unit] =
                    req.accepted(sink)
                      >> log.deleteAfter(req.prevLogIndex)
                      >> log.append(req.term, entries).void

                  val onNotMatches: F[Unit] =
                    req.inconsistent(sink)

                  matches.ifM(onMatches, onNotMatches)

          case (_, IncomingAppend(req, sink)) =>
            ResettableTimeout.outputPure(state.toFollower(req.term, req.leaderId))
    yield out
