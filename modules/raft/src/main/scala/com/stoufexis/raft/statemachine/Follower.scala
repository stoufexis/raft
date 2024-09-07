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
    Stream
      .eval(timeout.nextElectionTimeout)
      .flatMap: electionTimeout =>
        raceFirst(
          handleIncomingAppends(state, electionTimeout),
          handleIncomingVotes(state),
          handleClientRequests(state)
        )

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
          .ifM(req.accepted(sink), req.inconsistent(sink))
          .->(ResettableTimeout.Reset())

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
    rpc.incomingVotes.evalMapAccumulateFirstSome(Option.empty[NodeId]):
      case (votedFor, IncomingVote(req, sink)) if state.isExpired(req.term) =>
        req.termExpired(state, sink) as (votedFor, None)

      /** This candidate has our vote already, grant again.
        */
      case (Some(vf), IncomingVote(req, sink)) if state.isCurrent(req.term) && vf == req.candidateId =>
        req.grant(sink) as (Some(vf), None)

      /** We voted for someone else already.
        */
      case (Some(vf), IncomingVote(req, sink)) if state.isCurrent(req.term) =>
        req.reject(sink) as (Some(vf), None)

      /**
        * This candidate requested a vote first, grant it.
        */
      case (None, IncomingVote(req, sink)) if state.isCurrent(req.term) =>
        req.grant(sink) as (Some(req.candidateId), None)

      /** Election for new term. Vote after the transition.
        */
      case (vf, IncomingVote(req, sink)) =>
        F.pure(vf, Some(state.toFollowerUnknownLeader(req.term)))
