package com.stoufexis.leader.rpc

import cats.effect.kernel.*
import cats.implicits.given
import fs2.Stream

import com.stoufexis.leader.model.*
import com.stoufexis.leader.util.*

import scala.concurrent.duration.FiniteDuration

/** Should handle retries. Should make sure that received messages are from nodes within the
  * cluster, so only known NodeIds.
  */
trait RPC[F[_]]:
  def voteRequest(to: NodeId, request: VoteRequest): F[VoteResponse]

  def heartbeatRequest(to: NodeId, request: HeartbeatRequest): F[HeartbeatResponse]

  def incomingVoteRequests: Stream[F, IncomingVoteRequest[F]]

  def incomingHeartbeatRequests: Stream[F, IncomingHeartbeat[F]]

object RPC:
  def joinForEach[F[_]: Concurrent, A](
    tos: Set[NodeId]
  )(
    f: NodeId => Stream[F, A]
  ): Stream[F, (NodeId, A)] =
    Stream
      .iterable(tos)
      .map(to => f(to).map((to, _)))
      .parJoinUnbounded

  /** TODO: handle rpc errors
    */
  extension [F[_]: Temporal](rpc: RPC[F])
    def broadcastHeartbeat(
      term:        Term,
      nodes:       Nodes,
      repeatEvery: FiniteDuration
    ): Stream[F, (NodeId, HeartbeatResponse)] =
      joinForEach(nodes.otherNodes): to =>
        repeatOnInterval(
          repeatEvery,
          rpc.heartbeatRequest(to, HeartbeatRequest(nodes.currentNode, term))
        )

    def broadcastVote(
      term:        Term,
      nodes:       Nodes,
      repeatEvery: FiniteDuration
    ): Stream[F, (NodeId, VoteResponse)] =
      joinForEach(nodes.otherNodes): to =>
        repeatOnInterval(
          repeatEvery,
          rpc.voteRequest(to, VoteRequest(nodes.currentNode, term))
        )
