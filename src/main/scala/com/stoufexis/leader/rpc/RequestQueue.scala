package com.stoufexis.leader.rpc

import cats.effect.implicits.given
import cats.effect.kernel.*
import cats.effect.std.*
import cats.implicits.given
import fs2.*
import fs2.concurrent.SignallingRef

import scala.collection.immutable.SortedMap

/** A request is committed, and thus dequeued, only if a response is produced. Any later consumers will
  * see the same request. Used to implement RPC.
  * 
  * TODO: Test
  */
trait RequestQueue[F[_], I, O]:
  def offer(input: I): F[O]

  def consume: Stream[F, (I, Deferred[F, O])]

object RequestQueue:
  def bounded[F[_], I, O](bound: Int)(using
    F: Concurrent[F]
  ): F[RequestQueue[F, I, O]] =
    for
      map: SignallingRef[F, (Int, SortedMap[Int, (I, Deferred[F, O])])] <-
        SignallingRef[F].of(0, SortedMap.empty[Int, (I, Deferred[F, O])])

      sem: Semaphore[F] <-
        Semaphore[F](bound)
    yield new RequestQueue[F, I, O]:
      def consume: Stream[F, (I, Deferred[F, O])] =
        map
          .discrete
          .map(_._2)
          .mapAccumulate(Set.empty[Int]): (prev, next) =>
            (next.keySet, next.filterNot(x => prev.contains(x._1)))
          .flatMap: (_, newElems) =>
            Stream.iterable(newElems.values)

      def set(input: I, deferred: Deferred[F, O]): F[Int] =
        map.modify: (i, map) =>
          val i2: Int =
            if i == Int.MaxValue then 0 else i + 1

          val updated: SortedMap[Int, (I, Deferred[F, O])] =
            map.updated(i2, (input, deferred))

          ((i2, updated), i2)

      def unset(key: Int): F[Unit] =
        map.update((i, m) => (i, m.removed(key)))

      def offer(input: I): F[O] =
        sem.permit.surround:
          F.deferred[O].flatMap: deferred =>
            set(input, deferred).flatMap(k => deferred.get.guarantee(unset(k)))
