package com.stoufexis.leader.rpc

import com.stoufexis.leader.model.NodeId
import fs2.Stream

trait RPC[F[_]]:
  def voteBroadcast(request: VoteRequest): Stream[F, (NodeId, VoteResponse)]

  def voteRequest(to: NodeId, request: VoteRequest): F[VoteResponse]

  def heartbeatBroadcast(request: HeartbeatRequest): Stream[F, (NodeId, HeartbeatResponse)]

  def heartbeatRequest(to: NodeId, request: HeartbeatRequest): F[HeartbeatResponse]

  def incomingVoteRequests: Stream[F, IncomingVoteRequest[F]]

  def incomingHeartbeatRequests: Stream[F, IncomingHeartbeat[F]]

