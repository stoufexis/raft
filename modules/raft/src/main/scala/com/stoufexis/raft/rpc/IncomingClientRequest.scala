package com.stoufexis.raft.rpc

import cats.effect.kernel.DeferredSink

import com.stoufexis.raft.model.Command

case class IncomingClientRequest[F[_], In, Out, S](
  entry: Command[In],
  sink:  DeferredSink[F, ClientResponse[Out, S]]
)
