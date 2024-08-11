package com.stoufexis.leader.rpc

import cats.effect.kernel.DeferredSink

case class IncomingVoteRequest[F[_]](
  request: VoteRequest,
  sink:    DeferredSink[F, VoteResponse]
)
