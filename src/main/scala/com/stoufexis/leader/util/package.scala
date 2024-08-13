package com.stoufexis.leader.util

import cats.*
import cats.effect.kernel.*
import cats.implicits.given
import fs2.Stream
import org.typelevel.log4cats.Logger

import scala.concurrent.duration.FiniteDuration

extension [F[_]](l: Logger[F])
  def logDropped(str: String): F[Unit] =
    l.warn(s"Message dropped: $str")

extension [F[_]: Monad, A](deferred: DeferredSink[F, A])
  def complete_(a: A): F[Unit] = deferred.complete(a).void

extension [F[_]: Monad](deferred: DeferredSink[F, Unit])
  def complete_ : F[Unit] = deferred.complete(()).void

extension [F[_], A](stream: Stream[F, A])
  def evalMapFilterAccumulate[S, B](s: S)(f: (S, A) => F[(S, Option[B])]): Stream[F, B] =
    stream.evalMapAccumulate(s)(f).mapFilter(_._2)

extension [F[_], A](stream: Stream[F, A])(using fs2.Compiler[F, F], MonadThrow[F])
  def firstOrError: F[A] =
    stream.take(1).compile.lastOrError

def repeatOnInterval[F[_]: Temporal, A](
  delay: FiniteDuration,
  fa:    F[A]
): Stream[F, A] =
  (Stream.unit ++ Stream.fixedDelay(delay)) >> Stream.eval(fa)
