package com.stoufexis.leader.rpc

import fs2.Stream
import com.stoufexis.leader.model.NodeId
import cats.effect.kernel.*

trait RPC[F[_], A]:
  def requestVote(node: NodeId, request: RequestVote[A]): F[VoteResponse[A]]

  // TODO: Simplify
  def incomingRequests: Stream[F, (NodeId, RequestVote[A], DeferredSink[F, VoteResponse[A]])]