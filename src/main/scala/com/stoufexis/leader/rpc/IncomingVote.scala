package com.stoufexis.leader.rpc

import cats.effect.kernel.DeferredSink

case class IncomingVote[F[_]](
  request: RequestVote,
  sink:    DeferredSink[F, VoteResponse]
)
