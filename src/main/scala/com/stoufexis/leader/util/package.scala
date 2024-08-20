package com.stoufexis.leader.util

import cats.*
import cats.effect.kernel.*
import cats.implicits.given
import fs2.*

import scala.concurrent.duration.FiniteDuration

extension [F[_]: Monad, A](deferred: DeferredSink[F, A])
  def complete_(a: A): F[Unit] = deferred.complete(a).void

extension [F[_]: Monad](deferred: DeferredSink[F, Unit])
  def complete_ : F[Unit] = deferred.complete(()).void

enum ResettableTimeout[A] derives CanEqual:
  case Reset[A]()        extends ResettableTimeout[A]
  case Skip[A]()         extends ResettableTimeout[A]
  case Output[A](out: A) extends ResettableTimeout[A]

  def fold[B](onReset: => B, onSkip: => B, onOutput: A => B): B =
    this match
      case Reset()     => onReset
      case Skip()      => onSkip
      case Output(out) => onOutput(out)

extension [F[_], A](stream: Stream[F, A])
  // TODO: Test
  def resettableTimeoutAccumulate[S, B](
    init:      S,
    timeout:   FiniteDuration,
    onTimeout: F[B]
  )(f: (S, A) => F[(S, ResettableTimeout[B])])(using Temporal[F]): Stream[F, B] =
    def go(leftover: Chunk[A], timed: Pull.Timed[F, A], s: S): Pull[F, B, Unit] =
      val reset: Pull[F, Nothing, Unit] = timed.timeout(timeout)

      leftover match
        case chunk if chunk.nonEmpty =>
          for
            (s2, res) <- Pull.eval(f(s, chunk.head.get))
            _         <- res.fold(reset, Pull.pure(()), x => reset >> Pull.output1(x))
            _         <- go(chunk.drop(1), timed, s2)
          yield ()

        case chunk =>
          timed.uncons.flatMap:
            case None =>
              Pull.done

            case Some((Right(chunk), next)) =>
              go(chunk, next, s)

            case Some((Left(_), next)) =>
              Pull.eval(onTimeout).flatMap: b =>
                Pull.output1(b) >> reset >> go(Chunk.empty, next, s)
    end go

    stream.pull
      .timed(t => t.timeout(timeout) >> go(Chunk.empty, t, init))
      .stream

  def resettableTimeout[B](
    timeout:   FiniteDuration,
    onTimeout: F[B]
  )(f: A => F[ResettableTimeout[B]])(using Temporal[F]): Stream[F, B] =
    resettableTimeoutAccumulate((), timeout, onTimeout): (_, a) =>
      f(a).map(((), _))

def raceFirstOrError[F[_]: Concurrent, A](streams: List[Stream[F, A]]): F[A] =
  Stream
    .iterable(streams.map(_.take(1)))
    .parJoinUnbounded
    .take(1)
    .compile
    .lastOrError

def repeatOnInterval[F[_]: Temporal, A](
  delay: FiniteDuration,
  fa:    F[A]
): Stream[F, A] =
  (Stream.unit ++ Stream.fixedDelay(delay)) >> Stream.eval(fa)
