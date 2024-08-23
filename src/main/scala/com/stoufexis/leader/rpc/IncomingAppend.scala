package com.stoufexis.leader.rpc

import cats.effect.kernel.DeferredSink

case class IncomingAppend[F[_]](
  request: AppendEntries,
  sink:    DeferredSink[F, AppendResponse]
)