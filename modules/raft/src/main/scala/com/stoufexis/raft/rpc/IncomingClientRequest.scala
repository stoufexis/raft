package com.stoufexis.raft.rpc

import cats.effect.kernel.DeferredSink
import fs2.*

case class IncomingClientRequest[F[_], A, S](
  entries: Chunk[A],
  sink:    DeferredSink[F, ClientResponse[S]]
)
