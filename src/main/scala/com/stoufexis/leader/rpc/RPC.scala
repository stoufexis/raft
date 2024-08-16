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
  extension [F[_]: Temporal](rpc: RPC[F])
    def broadcastHeartbeat(
      term:        Term,
      nodes:       Set[NodeId],
      currentNode: NodeId,
      repeatEvery: FiniteDuration
    ): Stream[F, (NodeId, HeartbeatResponse)] =
      Stream
        .iterable(nodes - currentNode)
        .map: to =>
          repeatOnInterval(
            repeatEvery,
            rpc.heartbeatRequest(to, HeartbeatRequest(currentNode, term)).map((to, _))
          )
        .parJoinUnbounded
