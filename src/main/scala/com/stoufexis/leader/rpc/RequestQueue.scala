package com.stoufexis.leader.rpc

import cats.effect.implicits.given
import cats.effect.kernel.*
import cats.effect.kernel.Unique.Token
import cats.effect.std.*
import cats.effect.std.PQueue
import cats.implicits.given
import fs2.*
import fs2.concurrent.Channel
import fs2.concurrent.SignallingMapRef
import fs2.concurrent.SignallingRef

import scala.collection.immutable.SortedMap

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
  class Uncommitted[F[_], A](
    uncommitted: Dequeue[F, (Int, A)],
    idxs:        SignallingRef[F, (Int, Int)],
    concurrency: Int,
    semaphore:   Semaphore[F],
    bound:       Int
  )(using F: Concurrent[F]):
    // TODO handle idx overflows
    def tryOffer(a: A): F[Boolean] = semaphore.permit.surround:
      def shift(i: Int): F[Unit] =
        uncommitted
          .offerBack(i, a)
          .guarantee(uncommitted.takeFront.void)

      idxs.flatModify:
        case (cidx, ucidx) if ucidx - cidx <= bound => (cidx, ucidx + 1) -> shift(ucidx + 1).as(true)
        case (cidx, ucidx)                          => (cidx, ucidx) -> F.pure(false)

    def waitUntilCanOffer: F[Unit] =
      idxs.waitUntil((cidx, ucidx) => ucidx - cidx <= bound)

    def commit(token: Int): F[Boolean] = semaphore.permit.surround:
      idxs.modify:
        case (cidx, ucidx) if cidx < token => ((token, ucidx), true)
        case (cidx, ucidx)                 => ((cidx, ucidx), false)

    // Stops any committing and offerring, dequeues all elements (to pass them downstream) then requeues them
    // this function will be called only when a consumer is changing, which should happen infrequently
    // thus, this is inefficient because this utility is optimised for tryOffer and commit
    def readUncommitted(exec: F[Unit]): F[Chunk[A]] =
      val permits: Resource[F, Unit] =
        Resource.make(semaphore.acquireN(concurrency))(_ => semaphore.releaseN(concurrency))

      permits.surround:
        F.uncancelable: poll =>
          for 
            all <- poll(uncommitted.tryTakeFrontN(None))
            _   <- poll(exec)
            _   <- all.traverse_(uncommitted.offer)
          yield Chunk.iterable(all.map(_._2))


  def apply[F[_], I, O](bound: Int)(using F: Concurrent[F]): Stream[F, RequestQueue[F, I, O]] =
    for
      chan: Channel[F, (I, DeferredSink[F, O])] <-
        Stream.eval(Channel.unbounded)

      waiting: Uncommitted[F, (I, DeferredSink[F, O])] =
        ???
    yield new RequestQueue[F, I, O]:
      def offer(input: I): F[O] = ???
      // for
      //   deferred <- F.deferred[O]
      //   token    <- waiting.offer(input, deferred)
      //   o        <- deferred.get.guarantee(waiting.commit(token).void)
      // yield o

      def consume: Stream[F, (I, DeferredSink[F, O])] =
        ???
        // Stream.evalUnChunk(waiting.readUncommitted) ++ chan.stream

  // def bounded[F[_], I, O](bound: Int)(using F: Concurrent[F]): Resource[F, RequestQueue[F, I, O]] =

  //   def raceMap(head: (Int, (I, Deferred[F, O])), map: Map[Int, (I, Deferred[F, O])]): F[Int] =
  //     ???

  //   for
  //     inputs: Queue[F, (I, Deferred[F, O])] <-
  //       Resource.eval(Queue.synchronous)

  //     outputQueue: Queue[F, Channel[F, (I, Deferred[F, O])]] <-
  //       Resource.eval(Queue.unbounded)

  //     sup: Supervisor[F] <-
  //       Supervisor[F](await = false)

  //     background: F[Unit] =
  //       def go(
  //         waiting: SortedMap[Int, (I, Deferred[F, O])],
  //         output:  Channel[F, (I, Deferred[F, O])],
  //         idx:     Int
  //       ): F[Unit] =
  //         val inputOutputQueue: F[Either[(I, Deferred[F, O]), Channel[F, (I, Deferred[F, O])]]] =
  //           if waiting.size <= bound then
  //             F.race(inputs.take, outputQueue.take)
  //           else // Dont attempt to take from input if bound is reached
  //             outputQueue.take.map(Right(_))

  //         val raced: F[Either[Int, Either[(I, Deferred[F, O]), Channel[F, (I, Deferred[F, O])]]]] =
  //           waiting.headOption match
  //             case None =>
  //               inputOutputQueue.map(Right(_))

  //             case Some(head) =>
  //               F.race(raceMap(head, waiting.tail), inputOutputQueue)

  //         raced.flatMap:
  //           // A deferred completed
  //           case Left(i) =>
  //             go(waiting.removed(i), output, idx)

  //           // New input
  //           case Right(Left(inp)) =>
  //             output.send(inp) >> go(waiting.updated(idx + 1, inp), output, idx + 1)

  //           // Consumer change
  //           case Right(Right(newOutput)) =>
  //             waiting.traverse_(newOutput.send) >> go(waiting, newOutput, idx)

  //       outputQueue.take.flatMap(go(SortedMap.empty, _, 0))

  //     _ <-
  //       Resource.eval(sup.supervise(background))
  //   yield new RequestQueue[F, I, O]:
  //     def offer(input: I): F[DeferredSource[F, O]] =
  //       F.deferred[O]
  //         .flatTap(inputs.offer(input, _))
  //         .widen

  //     def consume: Stream[F, (I, DeferredSink[F, O])] =
  //       Stream
  //         .eval(Channel.unbounded[F, (I, Deferred[F, O])])
  //         .flatMap: c =>
  //           (Stream.eval(outputQueue.offer(c)) >> c.stream).onFinalize(c.close.void)
