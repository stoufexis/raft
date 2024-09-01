package com.stoufexis.leader.rpc

import cats.effect.implicits.given
import cats.effect.kernel.*
import cats.effect.std.*
import cats.implicits.given
import fs2.*
import fs2.concurrent.*

import scala.collection.immutable.Queue as ScalaQueue
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
          i <- poll(unprocessed.optimisticOffer(input, d))
          o <- poll(d.get).guarantee(unprocessed.drop(i))
        yield o

      def consume: Stream[F, (I, DeferredSink[F, O])] =
        unprocessed.read

  // TODO: test cancellations and stuff
  class Unprocessed[F[_], A](
    ref:   SignallingRef[F, State[F, A]],
    bound: Int
  )(using F: Concurrent[F]):
    // TODO handle idx overflows
    def tryOffer(a: A): F[Option[Int]] =
      ref.modify:
        case state if state.unprocessedSize >= bound =>
          (state, None)
        case state =>
          state.addUnprocessed(a).fmap(Some(_))

    def optimisticOffer(a: A): F[Int] =
      tryOffer(a).flatMap:
        case Some(i) => F.pure(i)
        case None    => offer(a)

    def offer(a: A): F[Int] =
      F.uncancelable: poll =>
        F.deferred[Unit].flatMap: offerrer =>
          val cleanup: F[Unit] = ref.flatModify: state =>
            state.waits.find(_ eq offerrer) match
              // Someone cleaned us up, wakeup the next one
              case None =>
                if state.waits.isEmpty then
                  state -> F.unit
                else
                  val (offerrer, rest) = state.waits.dequeue
                  state.copy(waits = rest) -> offerrer.complete(()).void

              case Some(_) =>
                state.copy(waits = state.waits.filter(_ ne offerrer)) -> F.unit

          ref.flatModify:
            case state if state.unprocessedSize >= bound =>
              state.addWaiting(offerrer) -> poll(offerrer.get >> offer(a)).onCancel(cleanup)
            case state =>
              state.addUnprocessed(a).fmap(F.pure)

    def drop(idx: Int): F[Unit] =
      F.uncancelable: _ =>
        ref.flatModify: state =>
          val s2 = state.dropUnprocessed(idx)

          if s2.waits.isEmpty then
            s2 -> F.unit
          else
            val (offerrer, rest) = state.waits.dequeue
            s2.copy(waits = rest) -> offerrer.complete(()).void

    // TODO: test
    def read(using ClassTag[A]): Stream[F, A] =
      ref
        .discrete
        .filterWithPrevious(_.idx < _.idx)
        .mapAccumulate(0): (lastEmitted, state) =>
          (state.idx, state.elemsBetween(lastEmitted + 1, state.idx))
        .flatMap((_, c) => Stream.chunk(c))

  object Unprocessed:
    def apply[F[_]: Concurrent, A](bound: Int): F[Unprocessed[F, A]] =
      for
        ref <- SignallingRef[F].of[State[F, A]](State.empty)
        // waits <- Queue.unbounded[F, Deferred[F, Unit]]
      yield new Unprocessed(ref, bound)

  case class State[F[_], A](idx: Int, elems: Map[Int, A], waits: ScalaQueue[Deferred[F, Unit]]):
    def unprocessedSize: Int =
      elems.size

    def addUnprocessed(a: A): (State[F, A], Int) =
      val i2 = idx + 1
      copy(idx = i2, elems = elems.updated(i2, a)) -> i2

    def dropUnprocessed(idx: Int): State[F, A] =
      copy(elems = elems.removed(idx))

    def addWaiting(offerrer: Deferred[F, Unit]): State[F, A] =
      copy(waits = waits.enqueue(offerrer))

    def elemsBetween(start: Int, end: Int)(using ClassTag[A]): Chunk[A] =
      val arr: ArrayBuilder[A] = ArrayBuilder.make
      (start to end).foreach(elems.get(_).foreach(arr.addOne))
      Chunk.array(arr.result())

  object State:
    def empty[F[_], A]: State[F, A] = State(0, Map.empty, ScalaQueue.empty)
