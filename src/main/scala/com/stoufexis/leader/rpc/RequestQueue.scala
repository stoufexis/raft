package com.stoufexis.leader.rpc

import cats.effect.implicits.given
import cats.effect.kernel.*
import cats.effect.std.*
import cats.implicits.given
import fs2.*
import fs2.concurrent.*

import scala.collection.mutable.ArrayBuilder
import scala.reflect.ClassTag

/** A request is committed, and thus dequeued, only if a response is produced. Any later consumers will
  * see the same request. Used to implement RPC.
  *
  * TODO: Test
  */
trait RequestQueue[F[_], I, O]:
  def offer(input: I): F[O]

  /** Someone needs to always be consuming, otherwise offer requests will hang forever
    */
  def consume: Stream[F, (I, DeferredSink[F, O])]

object RequestQueue:
  def apply[F[_], I, O](bound: Int)(using F: Concurrent[F]): F[RequestQueue[F, I, O]] =
    for
      unprocessed <- Unprocessed[F, (I, DeferredSink[F, O])](bound)
    yield new:
      def offer(input: I): F[O] = F.uncancelable: poll =>
        for
          d <- F.deferred[O]
          i <- poll(unprocessed.offer(input, d))
          o <- poll(d.get).guarantee(unprocessed.drop(i))
        yield o

      def consume: Stream[F, (I, DeferredSink[F, O])] =
        unprocessed.read

  // TODO: test cancellations and stuff
  class Unprocessed[F[_], A](
    ref:   SignallingRef[F, State[A]],
    waits: Queue[F, Deferred[F, Unit]],
    bound: Int
  )(using F: Concurrent[F]):

    def wakeupNext: F[Unit] =
      waits.tryTake.flatMap:
        case None       => F.unit
        case Some(head) => head.complete(()).ifM(F.unit, wakeupNext)

    // TODO handle idx overflows
    def tryOffer(a: A): F[Option[Int]] =
      ref.modify:
        case state if state.unprocessedSize >= bound =>
          (state, None)
        case state =>
          state.addUnprocessed(a).fmap(Some(_))

    def offer(a: A): F[Int] =
      def waitAndReoffer: F[Int] =
        F.deferred[Unit].flatMap: d =>
          val waitAndRetry: F[Int] =
            waits.offer(d) >> d.get >> offer(a)

          waitAndRetry.guarantee(d.complete(()).void)

      tryOffer(a).flatMap:
        case None      => waitAndReoffer
        case Some(idx) => F.pure(idx)

    def drop(idx: Int): F[Unit] =
      F.uncancelable: poll =>
        poll(ref.update(_.dropUnprocessed(idx))) >> wakeupNext

    // TODO: test
    def read(using ClassTag[A]): Stream[F, A] =
      ref
        .discrete
        .filterWithPrevious(_.idx != _.idx)
        .mapAccumulate(0): (lastEmitted, state) =>
          (state.idx, Stream.chunk(state.elemsBetween(lastEmitted, state.idx)))
        .flatMap(_._2)

  object Unprocessed:
    def apply[F[_]: Concurrent, A](bound: Int): F[Unprocessed[F, A]] =
      for
        ref   <- SignallingRef[F].of[State[A]](State.empty)
        waits <- Queue.unbounded[F, Deferred[F, Unit]]
      yield new Unprocessed(ref, waits, bound)

  case class State[A](idx: Int, elems: Map[Int, A]):
    def unprocessedSize: Int =
      elems.size

    def addUnprocessed(a: A): (State[A], Int) =
      val i2 = idx + 1
      copy(idx = i2, elems = elems.updated(i2, a)) -> i2

    def dropUnprocessed(idx: Int): State[A] =
      copy(elems = elems.removed(idx))

    // def addWaiting(waiting: Deferred[F, Unit]): State[F, A] =
    //   copy(waits = waits.enqueue(waiting))

    def elemsBetween(start: Int, end: Int)(using ClassTag[A]): Chunk[A] =
      val arr: ArrayBuilder[A] = ArrayBuilder.make
      (start to end).foreach(elems.get(_).foreach(arr.addOne))
      Chunk.array(arr.result())

  object State:
    def empty[A]: State[A] = State(0, Map.empty)
