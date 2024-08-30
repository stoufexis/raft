package com.stoufexis.leader.rpc

import cats.effect.implicits.given
import cats.effect.kernel.*
import cats.effect.kernel.Unique.Token
import cats.effect.std.*
import cats.implicits.given
import fs2.*
import fs2.concurrent.SignallingRef

trait RequestQueueSource[F[_], I, O]:
  def consume: Stream[F, (I, Deferred[F, O])]

trait RequestQueueSink[F[_], I, O]:
  def offer(input: I): F[O]

/** A request is committed, and thus dequeued, only if a response is produced. Any later consumers will
  * see the same request. Used to implement RPC.
  */
trait RequestQueue[F[_], I, O]
    extends RequestQueueSource[F, I, O]
    with RequestQueueSink[F, I, O]

object RequestQueue:
  def bounded[F[_], I, O](bound: Int, shardCount: Int)(using
    F: Concurrent[F]
  ): F[RequestQueue[F, I, O]] =
    for
      map: MapRef[F, Token, Option[(I, Deferred[F, O])]] <-
        MapRef.ofShardedImmutableMap(shardCount)

      tokens: SignallingRef[F, Set[Token]] <-
        SignallingRef[F].of(Set.empty)

      sem: Semaphore[F] <-
        Semaphore[F](bound)
    yield new RequestQueue[F, I, O]:
      def consume: Stream[F, (I, Deferred[F, O])] =
        tokens
          .discrete
          .mapAccumulate(Set.empty[Token])((prev, next) => (next, next.diff(prev)))
          .flatMap((_, newTokens) => Stream.iterable(newTokens.toList).evalMapFilter(map(_).get))

      def set(token: Token, input: I, deferred: Deferred[F, O]): F[Unit] =
        map(token).set(Some(input, deferred))

      def unset(token: Token): F[Unit] =
        map(token).set(None)

      def offer(input: I): F[O] = sem.permit.surround:
        for
          deferred: Deferred[F, O] <-
            Deferred[F, O]

          token: Token <-
            F.unique

          actions: F[O] =
            set(token, input, deferred) >> deferred.get

          o: O <-
            actions.guarantee(unset(token))
        yield o
