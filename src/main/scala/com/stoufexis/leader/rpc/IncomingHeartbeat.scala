package com.stoufexis.leader.rpc

import cats.effect.kernel.DeferredSink

case class IncomingHeartbeat[F[_]](
  request: HeartbeatRequest,
  sink:    DeferredSink[F, HeartbeatResponse]
)