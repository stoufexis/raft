package com.stoufexis.leader.rpc

import cats.effect.kernel.DeferredSink

case class IncomingAppend[F[_], A](
  request: AppendEntries[A],
  sink:    DeferredSink[F, AppendResponse]
)