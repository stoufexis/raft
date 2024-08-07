package com.stoufexis.leader.rpc

import cats.effect.kernel.*
import com.stoufexis.leader.model.NodeId
import fs2.Stream

trait RPC[F[_], A]:
  def voteRequest(to: NodeId, request: VoteRequest[A]): F[VoteResponse]

  def heartbeatRequest(to: NodeId, request: HeartbeatRequest[A]): F[HeartbeatResponse]

  // TODO: Simplify
  def incomingVoteRequests: Stream[F, (VoteRequest[A], DeferredSink[F, VoteResponse])]

  def incomingHeartbeatRequests: Stream[F, (HeartbeatRequest[A], DeferredSink[F, HeartbeatResponse])]

