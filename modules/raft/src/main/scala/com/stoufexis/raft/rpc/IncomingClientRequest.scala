package com.stoufexis.raft.rpc

import cats.effect.kernel.DeferredSink

import com.stoufexis.raft.model.Command

case class IncomingClientRequest[F[_], A, S](
  entry: Option[Command[A]],
  sink:  DeferredSink[F, ClientResponse[S]]
)
