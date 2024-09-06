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

def solicitVotes[F[_], A, S](
  state:           NodeInfo[S],
  electionTimeout: FiniteDuration
)(using
  F:   Temporal[F],
  log: Logger[F],
  rpc: RPC[F, A, S]
): Stream[F, NodeInfo[S]] =
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
        (newNodes, success, ResettableTimeout.Output(state.toLeader))
      else
        (newNodes, voted, ResettableTimeout.Skip())

    case (nodes, (node, VoteResponse.Rejected)) =>
      (nodes, log.info(s"Node $node rejected vote"), ResettableTimeout.Skip())

    case (nodes, (_, VoteResponse.TermExpired(newTerm))) =>
      (nodes, log.warn(s"Detected stale term"), ResettableTimeout.Output(state.toFollower(newTerm)))

    case (nodes, (_, VoteResponse.IllegalState(msg))) =>
      (nodes, F.raiseError(IllegalStateException(msg)), ResettableTimeout.Skip())
