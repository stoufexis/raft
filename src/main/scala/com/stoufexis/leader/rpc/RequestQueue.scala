package com.stoufexis.leader.rpc

import cats.effect.kernel.*
import cats.effect.std.*
import cats.implicits.given
import fs2.*

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
  def consume: Stream[F, (I, Deferred[F, O])]

object RequestQueue:
  def bounded[F[_], I, O](bound: Int)(using F: Concurrent[F]): Resource[F, RequestQueue[F, I, O]] =

    def raceMap(head: (Int, (I, Deferred[F, O])), map: Map[Int, (I, Deferred[F, O])]): F[Int] =
      ???

    for
      inputs: Queue[F, (I, Deferred[F, O])] <-
        Resource.eval(Queue.synchronous)

      outputQueue: Queue[F, Queue[F, (I, Deferred[F, O])]] <-
        Resource.eval(Queue.synchronous)

      sup: Supervisor[F] <-
        Supervisor[F](await = false)

      background: F[Unit] =
        def go(
          waiting: SortedMap[Int, (I, Deferred[F, O])],
          output:  Queue[F, (I, Deferred[F, O])],
          idx:     Int
        ): F[Unit] =
          val inputOutputQueue: F[Either[(I, Deferred[F, O]), Queue[F, (I, Deferred[F, O])]]] =
            if waiting.size <= bound then
              F.race(inputs.take, outputQueue.take)
            else // Dont attempt to take from input if bound is reached
              outputQueue.take.map(Right(_))

          val raced: F[Either[Int, Either[(I, Deferred[F, O]), Queue[F, (I, Deferred[F, O])]]]] =
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
              output.offer(inp)
                >> go(waiting.updated(idx + 1, inp), output, idx + 1)

            // Consumer change
            case Right(Right(newOutput)) =>
              waiting.values.toList.traverse(newOutput.offer)
                >> go(waiting, newOutput, idx)

        outputQueue.take.flatMap(go(SortedMap.empty, _, 0))

      _ <-
        Resource.eval(sup.supervise(background))
    yield new RequestQueue[F, I, O]:
      def offer(input: I): F[O] =
        F.deferred[O].flatMap: d =>
          inputs.offer(input, d) >> d.get

      def consume: Stream[F, (I, Deferred[F, O])] =
        Stream.eval(Queue.unbounded[F, (I, Deferred[F, O])]).flatMap: q =>
          Stream.eval(outputQueue.offer(q)) >> Stream.fromQueueUnterminated(q)
