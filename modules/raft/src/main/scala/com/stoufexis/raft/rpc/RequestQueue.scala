package com.stoufexis.raft.rpc

import cats.effect.implicits.given
import cats.effect.kernel.*
import cats.implicits.given
import fs2.*
import fs2.concurrent.*

import scala.collection.immutable.Queue as ScalaQueue
import scala.reflect.ClassTag

/** A request is committed, and thus dequeued, only if a response is produced. Any later consumers will
  * see the same request. Used to implement RPC.
  *
  * TODO: Test
  */
trait RequestQueue[F[_], I, O]:

  /** Someone needs to always be consuming, otherwise offer requests will hang forever
    */
  def consume: Stream[F, (I, DeferredSink[F, O])]

  /** Offers most of the time succeed in a FIFO order, but if an offer has to wait for room, there are
    * race conditions that make this not 100% guaranteed all of the time. This is not handled by the
    * RequestQueue, since it is overhead thats not necessary for most use cases.
    *
    * If you want to guarantee the order of 2 offers, you have to do one of the following:
    *   - wait for the first one to finish before calling the second one
    *   - batch the offers in one
    *   - tag your requests with some index and process them in the correct order on the consumer side
    *
    * This also means that different clients cannot rely on comparing their request time to gauge the
    * order of execution of their requests. IE. if one `offer` is called before a second one, it is not
    * guaranteed that the requests these offers produce will be executed in the same order.
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
      F.deferred[Unit].flatMap: offerrer =>
        val cleanup: F[Unit] = ref.flatModify: state =>
          val (ours, others) =
            state.offerrers.partition(_ eq offerrer)

          ours.headOption match
            // Our offerrer was not in the offerrers so someone woke us up, but we were cancelled before succeeding in adding to the queue.
            // We need to wake up the next offerrer because we did not fill the empty spot ourselves.
            case None =>
              if ours.isEmpty then
                state -> F.unit
              else
                val (offerrer, rest) = ours.dequeue
                state.copy(offerrers = rest) -> offerrer.complete(()).void

            case Some(_) =>
              state.copy(offerrers = others) -> F.unit

        ref.flatModify:
          case state if state.elems.size >= bound =>
            state.copy(offerrers = state.offerrers.enqueue(offerrer))
              -> (offerrer.get >> offer(a)).onCancel(cleanup)

          case state =>
            // Assuming an average of 1000 requests a second, which is already too many,
            // this will overflow at just under 300_000_000 years. Nothing to be done about overflows...
            val i2 = state.idx + 1
            state.copy(idx = i2, elems = state.elems.updated(i2, a))
              -> F.pure(i2)

    def drop(idx: Long): F[Unit] =
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
        .mapAccumulate(0L): (lastEmitted, state) =>
          (state.idx, state.elemsBetween(lastEmitted + 1, state.idx))
        .flatMap((_, c) => Stream.chunk(c))

  object Unprocessed:
    def apply[F[_]: Concurrent, A](bound: Int): F[Unprocessed[F, A]] =
      for
        ref <- SignallingRef[F].of[State[F, A]](State.empty)
      yield new Unprocessed(ref, bound)

  case class State[F[_], A](idx: Long, elems: Map[Long, A], offerrers: ScalaQueue[Deferred[F, Unit]]):
    def elemsBetween(start: Long, end: Long)(using ClassTag[A]): Chunk[A] =
      Chunk.iterator(Iterator.range(start, end + 1).flatMap(elems.get))

  object State:
    def empty[F[_], A]: State[F, A] = State(0, Map.empty, ScalaQueue.empty)
