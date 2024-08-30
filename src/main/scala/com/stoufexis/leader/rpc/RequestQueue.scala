package com.stoufexis.leader.rpc

import cats.effect.kernel.*
import cats.effect.std.*
import cats.implicits.given
import fs2.*
import fs2.concurrent.Channel

import scala.collection.immutable.SortedMap

/** A request is committed, and thus dequeued, only if a response is produced. Any later consumers will
  * see the same request. Used to implement RPC.
  *
  * TODO: Test
  */
trait RequestQueue[F[_], I, O]:
  def offer(input: I): F[DeferredSource[F, O]]

  /** Someone needs to always be consuming, otherwise offer requests will hang forever
    */
  def consume: Stream[F, (I, DeferredSink[F, O])]

object RequestQueue:
  def bounded[F[_], I, O](bound: Int)(using F: Concurrent[F]): Resource[F, RequestQueue[F, I, O]] =

    def raceMap(head: (Int, (I, Deferred[F, O])), map: Map[Int, (I, Deferred[F, O])]): F[Int] =
      ???

    for
      inputs: Queue[F, (I, Deferred[F, O])] <-
        Resource.eval(Queue.synchronous)

      outputQueue: Queue[F, Channel[F, (I, Deferred[F, O])]] <-
        Resource.eval(Queue.unbounded)

      sup: Supervisor[F] <-
        Supervisor[F](await = false)

      background: F[Unit] =
        def go(
          waiting: SortedMap[Int, (I, Deferred[F, O])],
          output:  Channel[F, (I, Deferred[F, O])],
          idx:     Int
        ): F[Unit] =
          val inputOutputQueue: F[Either[(I, Deferred[F, O]), Channel[F, (I, Deferred[F, O])]]] =
            if waiting.size <= bound then
              F.race(inputs.take, outputQueue.take)
            else // Dont attempt to take from input if bound is reached
              outputQueue.take.map(Right(_))

          val raced: F[Either[Int, Either[(I, Deferred[F, O]), Channel[F, (I, Deferred[F, O])]]]] =
            waiting.headOption match
              case None =>
                inputOutputQueue.map(Right(_))

              case Some(head) =>
                F.race(raceMap(head, waiting.tail), inputOutputQueue)

          raced.flatMap:
            // A deferred completed
            case Left(i) =>
              go(waiting.removed(i), output, idx)

            // New input
            case Right(Left(inp)) =>
              output.send(inp) >> go(waiting.updated(idx + 1, inp), output, idx + 1)

            // Consumer change
            case Right(Right(newOutput)) =>
              waiting.traverse_(newOutput.send) >> go(waiting, newOutput, idx)

        outputQueue.take.flatMap(go(SortedMap.empty, _, 0))

      _ <-
        Resource.eval(sup.supervise(background))
    yield new RequestQueue[F, I, O]:
      def offer(input: I): F[DeferredSource[F, O]] =
        F.deferred[O]
          .flatTap(inputs.offer(input, _))
          .widen

      def consume: Stream[F, (I, DeferredSink[F, O])] =
        Stream
          .eval(Channel.unbounded[F, (I, Deferred[F, O])])
          .flatMap: c =>
            (Stream.eval(outputQueue.offer(c)) >> c.stream).onFinalize(c.close.void)
