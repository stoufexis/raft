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

extension [F[_], A](stream: Stream[F, A])
  def resettableTimeoutAccumulate[S, B](
    init:      S,
    timeout:   FiniteDuration,
    onTimeout: B
  )(f: (S, A) => F[(S, ResettableTimeout[B])])(using Temporal[F]): Stream[F, B] =
    stream
      .evalMapAccumulate(init): (s, a) =>
        f(s, a).map[(S, Option[Option[B]])]:
          case (s2, ResettableTimeout.Reset())   => (s2, Some(None))
          case (s2, ResettableTimeout.Skip())    => (s2, None)
          case (s2, ResettableTimeout.Output(b)) => (s2, Some(Some(b)))
      .mapFilter(_._2)
      .timeoutOnPullTo(timeout, Stream(Some(onTimeout)))
      .collect { case Some(b) => b }

  def resettableTimeout[B](
    timeout:   FiniteDuration,
    onTimeout: B
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
