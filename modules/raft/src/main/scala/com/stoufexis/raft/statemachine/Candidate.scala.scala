package com.stoufexis.raft.statemachine

import cats.*
import cats.effect.kernel.*
import cats.implicits.given
import fs2.*
import org.typelevel.log4cats.Logger

import com.stoufexis.raft.model.*
import com.stoufexis.raft.rpc.*

import scala.concurrent.duration.FiniteDuration

object Candidate:
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
          handleIncomingAppends(state),
          handleIncomingVotes(state),
          handleClientRequests(state),
          solicitVotes(state, electionTimeout)
        )

  def handleClientRequests[F[_], A, S](state: NodeInfo[S])(using rpc: RPC[F, A, S]): Stream[F, Nothing] =
    rpc
      .incomingClientRequests
      .evalMap(_.sink.complete(ClientResponse.knownLeader(state.knownLeader)))
      .drain

  def handleIncomingAppends[F[_], A, S](
    state: NodeInfo[S]
  )(using
    F:      MonadThrow[F],
    rpc:    RPC[F, A, S],
    logger: Logger[F]
  ): Stream[F, NodeInfo[S]] =
    rpc.incomingAppends.evalMapFirstSome:
      case IncomingAppend(req, sink) if state.isExpired(req.term) =>
        req.termExpired(state, sink) as None

      /** Someone else got elected before us. Recognise them. Append will be handled when transitioned to
        * follower. Incoming term may be current or a larger one, we always transition to follower.
        */
      case IncomingAppend(req, sink) =>
        F.pure(Some(state.toFollower(req.term, req.leaderId)))

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

      /** We have voted for ourselves this term, as we are a candidate.
        */
      case IncomingVote(req, sink) if state.isCurrent(req.term) =>
        req.reject(sink) as None

      /** Vote request for next term. Our term has expired, vote for the other candidate. Request will be
        * fulfilled when transitioned to follower.
        */
      case IncomingVote(req, sink) =>
        F.pure(Some(state.toVotedFollower(req.candidateId, req.term)))

  def solicitVotes[F[_], A, S](
    state:           NodeInfo[S],
    electionTimeout: FiniteDuration
  )(using
    F:   Temporal[F],
    log: Logger[F],
    rpc: RPC[F, A, S]
  ): Stream[F, NodeInfo[S]] =
    import ResettableTimeout.*

    val responses: Stream[F, (NodeId, VoteResponse)] =
      Stream.iterable(state.otherNodes).parEvalMapUnbounded: node =>
        rpc.requestVote(node, RequestVote(state.currentNode, state.term))
          .tupleLeft(node)

    responses.resettableTimeoutAccumulate(
      init      = Set(state.currentNode),
      onTimeout = F.pure(state.toCandidateNextTerm),
      timeout   = electionTimeout
    ):
      case (nodes, (node, VoteResponse.Granted)) =>
        val newNodes: Set[NodeId] =
          nodes + node

        val voted: F[Unit] =
          log.info(s"Node $node granted vote")

        val success: F[Unit] =
          log.info(s"Majority votes collected")

        if state.isMajority(newNodes) then
          (newNodes, success, Output(state.toLeader))
        else
          (newNodes, voted, Skip())

      case (nodes, (node, VoteResponse.Rejected)) =>
        (nodes, log.info(s"Node $node rejected vote"), Skip())

      case (nodes, (_, VoteResponse.TermExpired(newTerm))) =>
        (nodes, log.warn(s"Detected stale term"), Output(state.toFollowerUnknownLeader(newTerm)))

      case (nodes, (_, VoteResponse.IllegalState(msg))) =>
        (nodes, F.raiseError(IllegalStateException(msg)), Skip())
