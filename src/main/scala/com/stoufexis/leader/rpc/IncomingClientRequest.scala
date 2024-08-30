package com.stoufexis.leader.rpc

import cats.effect.kernel.DeferredSink
import fs2.*

case class IncomingClientRequest[F[_], A, S](
  entries: Chunk[A],
  sink:    DeferredSink[F, Option[S]]
)
