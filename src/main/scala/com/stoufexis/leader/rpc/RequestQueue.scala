package com.stoufexis.leader.rpc

import cats.effect.kernel.*
import fs2.*

trait RequestQueueSource[F[_], I, O]:
  def consume: Stream[F, (I, DeferredSink[F, O])]

trait RequestQueueSink[F[_], I, O]:
  def offer(input: I): F[DeferredSource[F, O]]

  def tryOffer(input: I): F[Option[DeferredSource[F, O]]]

/** A request is dequeued only if a response is produced. Any later consumers will see the same request.
  * Used to implement RPC
  */
trait RequestQueue[F[_], I, O]
    extends RequestQueueSource[F, I, O]
    with RequestQueueSink[F, I, O]
