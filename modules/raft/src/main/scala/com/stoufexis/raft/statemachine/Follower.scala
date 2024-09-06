package com.stoufexis.raft.statemachine

import cats.*
import cats.effect.kernel.*
import cats.implicits.given
import fs2.*
import org.typelevel.log4cats.Logger

import com.stoufexis.raft.model.*
import com.stoufexis.raft.rpc.*

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

      handleClient: Stream[F, Nothing] =
        handleClientRequests(state)

      out: NodeInfo[S] <-
        raceFirst(List(handleAppends, handleVotes, handleClient))
    yield out

  def handleClientRequests[F[_], A, S](state: NodeInfo[S])(using rpc: RPC[F, A, S]): Stream[F, Nothing] =
    rpc
      .incomingClientRequests
      .evalMap(_.sink.complete(ClientResponse.knownLeader(state.knownLeader)))
      .drain

  def handleIncomingAppends[F[_], A, S](
    state:           NodeInfo[S],
    electionTimeout: FiniteDuration
  )(using
    F:      Temporal[F],
    rpc:    RPC[F, A, S],
    log:    Log[F, A],
    logger: Logger[F]
  ): Stream[F, NodeInfo[S]] =
    rpc.incomingAppends.resettableTimeout(electionTimeout, F.pure(state.toCandidateNextTerm)):
      case IncomingAppend(req, sink) if state.isExpired(req.term) =>
        req.termExpired(state, sink) -> ResettableTimeout.Skip()

      case IncomingAppend(req, sink) if state.isCurrent(req.term) && state.isLeader(req.leaderId) =>
        log
          .appendChunkIfMatches(req.prevLogTerm, req.prevLogIndex, req.term, req.entries)
          .ifM(req.accepted(sink), req.inconsistent(sink)) -> ResettableTimeout.Reset()

      /** Let the request be fulfilled after we transition
        */
      case IncomingAppend(req, sink) =>
        F.unit -> ResettableTimeout.Output(state.toFollower(req.term, req.leaderId))

  def handleIncomingVotes[F[_], A, S](
    state: NodeInfo[S]
  )(using
    F:      MonadThrow[F],
    rpc:    RPC[F, A, S],
    logger: Logger[F]
  ): Stream[F, NodeInfo[S]] =
    rpc.incomingVotes.evalMapFirstSome:
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
