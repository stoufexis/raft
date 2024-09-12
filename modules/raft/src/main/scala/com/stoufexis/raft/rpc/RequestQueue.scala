package com.stoufexis.raft.rpc

import cats.effect.implicits.given
import cats.effect.kernel.*
import cats.implicits.given
import fs2.*
import fs2.concurrent.*

import scala.collection.immutable.Queue as ScalaQueue
import scala.reflect.ClassTag

/** A request is committed, and thus dequeued, only if a response is produced. Any later consumers will see
  * the same request. Used to implement RPC.
  *
  * TODO: Test
  */
trait RequestQueue[F[_], I, O]:

  /** Someone needs to always be consuming, otherwise offer requests will hang forever
    */
  def consume: Stream[F, (I, DeferredSink[F, O])]

  /** Offers succeed in FIFO order
    */
  def offer(input: I): F[O]

  def tryOffer(input: I): F[Option[O]]

object RequestQueue:
  def apply[F[_], I, O](bound: Int)(using F: Concurrent[F]): F[RequestQueue[F, I, O]] =
    for
      unprocessed <- Unprocessed[F, (I, DeferredSink[F, O])](bound)
    yield new:
      def consume: Stream[F, (I, DeferredSink[F, O])] =
        unprocessed.read

      def offer(input: I): F[O] = F.uncancelable: poll =>
        for
          d <- F.deferred[O]
          i <- poll(unprocessed.offer(input, d))
          o <- poll(d.get).guarantee(unprocessed.drop(i))
        yield o

      def tryOffer(input: I): F[Option[O]] = F.uncancelable: poll =>
        for
          d <- F.deferred[O]
          i <- poll(unprocessed.tryOffer(input, d))
          o <- i.traverse(i => poll(d.get).guarantee(unprocessed.drop(i)))
        yield o

  // TODO: test cancellations and stuff
  // Design heavily inspired by the cats-effect Queue implementation for Concurrent
  class Unprocessed[F[_], A](
    ref:   SignallingRef[F, State[F, A]],
    bound: Int
  )(using F: Concurrent[F]):
    def tryOffer(a: A): F[Option[Long]] =
      ref.modify:
        case state if state.elems.size >= bound =>
          (state, None)
        case state =>
          val i2 = state.idx + 1
          state.copy(idx = i2, elems = state.elems.updated(i2, a)) -> Some(i2)

    def offer(a: A): F[Long] =
      def loop(offerrer: Deferred[F, Unit]): F[Long] =
        F.uncancelable: poll =>
          val dequeueSelf: F[Unit] =
            ref.update(_.excludeOfferrer(offerrer))

          val dequeueSelfWakeupNext: F[Unit] =
            ref.flatModify:
              case state if state.elems.size < bound =>
                val newState: State[F, A] =
                  state.excludeOfferrer(offerrer)

                newState -> newState.nextOfferrer.match
                  case None       => F.unit
                  case Some(next) => next.complete(()).void

              case state =>
                state.excludeOfferrer(offerrer) -> F.unit

          ref.flatModify:
            case state if state.elems.size >= bound =>
              state.enqueueOfferrer(offerrer)
                -> poll(offerrer.get >> loop(offerrer)).onCancel(dequeueSelf)

            case state =>
              val (s, i2) = state.newElem(a)
              s -> dequeueSelfWakeupNext.as(i2)

      F.deferred[Unit].flatMap(loop)

    def drop(idx: Long): F[Unit] = F.uncancelable: _ =>
      ref.flatModify: state =>
        state.dropElem(idx) -> state.nextOfferrer.match
          case None           => F.unit
          case Some(offerrer) => offerrer.complete(()).void

    // TODO: test
    def read(using ClassTag[A]): Stream[F, A] =
      ref
        .discrete
        .filterWithPrevious(_.idx < _.idx)
        .mapAccumulate(0L): (lastEmitted, state) =>
          (state.idx, state.elemsBetween(lastEmitted + 1, state.idx))
        .flatMap((_, c) => Stream.chunk(c))

    def print: F[Unit] =
      ref.get.map(s => println(s.elems))

  object Unprocessed:
    def apply[F[_]: Concurrent, A](bound: Int): F[Unprocessed[F, A]] =
      for
        ref <- SignallingRef[F].of[State[F, A]](State.empty)
      yield new Unprocessed(ref, bound)

  case class State[F[_], A](idx: Long, elems: Map[Long, A], offerrers: ScalaQueue[Deferred[F, Unit]]):
    def elemsBetween(start: Long, end: Long)(using ClassTag[A]): Chunk[A] =
      Chunk.iterator(Iterator.range(start, end + 1).flatMap(elems.get))

    def excludeOfferrer(d: Deferred[F, Unit]): State[F, A] =
      copy(offerrers = offerrers.filterNot(_ eq d))

    def enqueueOfferrer(d: Deferred[F, Unit]): State[F, A] =
      copy(offerrers = offerrers.enqueue(d))

    def nextOfferrer: Option[Deferred[F, Unit]] =
      offerrers.dequeueOption.map(_._1)

    def newElem(elem: A): (State[F, A], Long) =
      // Assuming an average of 1000 requests a second, which is already too many,
      // this will overflow at just under 300_000_000 years. Nothing to be done about overflows...
      val i2 = idx + 1
      copy(idx = i2, elems = elems.updated(i2, elem)) -> i2

    def dropElem(idx: Long): State[F, A] =
      copy(elems = elems.removed(idx))

  object State:
    def empty[F[_], A]: State[F, A] = State(0, Map.empty, ScalaQueue.empty)
