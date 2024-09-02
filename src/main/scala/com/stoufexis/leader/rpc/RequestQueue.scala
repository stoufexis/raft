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
          i <- poll(unprocessed.offer(input, d))
          o <- poll(d.get).guarantee(unprocessed.drop(i))
        yield o

      def consume: Stream[F, (I, DeferredSink[F, O])] =
        unprocessed.read

  // TODO: test cancellations and stuff
  // Design heavily inspired by the cats-effect Queue implementation for Concurrent
  class Unprocessed[F[_], A](
    ref:   SignallingRef[F, State[F, A]],
    bound: Int
  )(using F: Concurrent[F]):
    // TODO handle idx overflows
    def tryOffer(a: A): F[Option[Int]] =
      ref.modify:
        case state if state.elems.size >= bound =>
          (state, None)
        case state =>
          val i2 = state.idx + 1
          state.copy(idx = i2, elems = state.elems.updated(i2, a)) -> Some(i2)

    def offer(a: A): F[Int] =
      F.deferred[Unit].flatMap: offerrer =>
        val cleanup: F[Unit] = ref.flatModify: state =>
          state.offerrers.find(_ eq offerrer) match
            // Someone woke us up, but we were cancelled before succeeding in offering -> wakeup the next one
            case None =>
              if state.offerrers.isEmpty then
                state -> F.unit
              else
                val (offerrer, rest) = state.offerrers.dequeue
                state.copy(offerrers = rest) -> offerrer.complete(()).void

            case Some(_) =>
              state.copy(offerrers = state.offerrers.filter(_ ne offerrer)) -> F.unit

        ref.flatModify:
          case state if state.elems.size >= bound =>
            state.copy(offerrers = state.offerrers.enqueue(offerrer))
              -> (offerrer.get >> offer(a)).onCancel(cleanup)
          case state =>
            val i2 = state.idx + 1
            state.copy(idx = i2, elems = state.elems.updated(i2, a))
              -> F.pure(i2)

    def drop(idx: Int): F[Unit] =
      ref.flatModify: state =>
        if state.offerrers.isEmpty then
          state.copy(elems = state.elems.removed(idx))
            -> F.unit
        else
          val (offerrer, rest) = state.offerrers.dequeue
          state.copy(elems = state.elems.removed(idx), offerrers = rest)
            -> offerrer.complete(()).void.uncancelable

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

  case class State[F[_], A](idx: Int, elems: Map[Int, A], offerrers: ScalaQueue[Deferred[F, Unit]]):
    def elemsBetween(start: Int, end: Int)(using ClassTag[A]): Chunk[A] =
      val arr: ArrayBuilder[A] = ArrayBuilder.make
      (start to end).foreach(elems.get(_).foreach(arr.addOne))
      Chunk.array(arr.result())

  object State:
    def empty[F[_], A]: State[F, A] = State(0, Map.empty, ScalaQueue.empty)
