package com.stoufexis.leader.statemachine

import cats.*
import cats.effect.kernel.*
import cats.implicits.given
import fs2.*
import org.typelevel.log4cats.Logger

import com.stoufexis.leader.model.*
import com.stoufexis.leader.rpc.*

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

      handleAppends: Stream[F, NodeInfo[S]] =
        handleIncomingAppends(state, electionTimeout)

      handleVotes: Stream[F, NodeInfo[S]] =
        handleIncomingVotes(state)

      out: NodeInfo[S] <-
        raceFirst(List(handleAppends, handleVotes))
    yield out

  def handleIncomingAppends[F[_], A, S](
    state:           NodeInfo[S],
    electionTimeout: FiniteDuration
  )(using
    F:      Temporal[F],
    rpc:    RPC[F, A, S],
    log:    Log[F, A],
    logger: Logger[F]
  ): Stream[F, NodeInfo[S]] =
    rpc.incomingAppends.resettableTimeout(
      electionTimeout,
      F.pure(state.toCandidateNextTerm)
    ):
      case IncomingAppend(req, sink) if state.isExpired(req.term) =>
        req.termExpired(state, sink) -> ResettableTimeout.Skip()

      case IncomingAppend(req, sink) if state.isCurrent(req.term) =>
        val response: F[Unit] =
          log.appendChunkIfMatches(req.prevLogTerm, req.prevLogIndex, req.term, req.entries).flatMap:
            case Some(newIdx) => req.accepted(sink)
            case None         => req.inconsistent(sink)

        response -> ResettableTimeout.Reset()

      /** Let the request be fulfilled when we transition
        */
      case IncomingAppend(req, sink) =>
        F.unit -> ResettableTimeout.Output(state.newTerm(req.term))

  def handleIncomingVotes[F[_], A, S](
    state: NodeInfo[S]
  )(using
    F:      MonadThrow[F],
    rpc:    RPC[F, A, S],
    logger: Logger[F]
  ): Stream[F, NodeInfo[S]] =
    rpc.incomingVotes.evalMapFilter:
      case IncomingVote(req, sink) if state.isExpired(req.term) =>
        req.termExpired(state, sink) as None

      /** This candidate has our vote already, grant again. This case is expected because we wait for
        * after the transition to grant a vote, so this request is being carried over from before the
        * transition. Our response may also have been lost for whatever reason.
        */
      case IncomingVote(req, sink) if state.isCurrent(req.term) && state.votedFor(req.candidateId) =>
        req.grant(sink) as None

      case IncomingVote(req, sink) if state.isCurrent(req.term) =>
        req.reject(sink) as None

      /** Election for new term. Vote after the transition.
        */
      case IncomingVote(req, sink) =>
        F.pure(Some(state.toVotedFollower(req.candidateId, req.term)))
