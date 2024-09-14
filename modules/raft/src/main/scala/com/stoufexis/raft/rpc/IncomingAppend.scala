package com.stoufexis.raft.rpc

import cats.effect.kernel.DeferredSink

case class IncomingAppend[F[_], In](
  request: AppendEntries[In],
  sink:    DeferredSink[F, AppendResponse]
)