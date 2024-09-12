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
          state.copy(idx = i2, elems = state.elems.updated(i2, a))
            -> Some(i2)

    def offer(a: A): F[Long] = F.uncancelable: poll =>
      F.deferred[Long].flatMap: offerrer =>
        val cleanup: F[Unit] =
          ref.update(s => s.copy(offerrers = s.offerrers.filterNot(_._2 eq offerrer)))

        ref.flatModify:
          case state if state.elems.size >= bound =>
            state.copy(offerrers = state.offerrers.enqueue(a, offerrer))
              -> poll(offerrer.get).onCancel(cleanup)

          case state =>
            // Assuming an average of 1000 requests a second, which is already too many,
            // this will overflow at just under 300_000_000 years. Nothing to be done about overflows...
            val i2 = state.idx + 1
            state.copy(idx = i2, elems = state.elems.updated(i2, a))
              -> F.pure(i2)

    def drop(idx: Long): F[Unit] = F.uncancelable: _ =>
      ref.flatModify: state =>
        if state.offerrers.isEmpty then
          state.copy(elems = state.elems.removed(idx))
            -> F.unit
        else
          val ((elem, sink), rest) = state.offerrers.dequeue
          val i                    = state.idx + 1
          val updated              = state.elems.removed(idx).updated(i, elem)
          state.copy(elems = updated, offerrers = rest, idx = i)
            -> sink.complete(i).void

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

  case class State[F[_], A](idx: Long, elems: Map[Long, A], offerrers: ScalaQueue[(A, Deferred[F, Long])]):
    def elemsBetween(start: Long, end: Long)(using ClassTag[A]): Chunk[A] =
      Chunk.iterator(Iterator.range(start, end + 1).flatMap(elems.get))

  object State:
    def empty[F[_], A]: State[F, A] = State(0, Map.empty, ScalaQueue.empty)
