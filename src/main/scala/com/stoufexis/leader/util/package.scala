package com.stoufexis.leader.util

import cats.Monad
import cats.effect.Temporal
import cats.effect.kernel.DeferredSink
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

def repeatOnInterval[F[_]: Temporal, A](
  delay: FiniteDuration,
  fa:    F[A]
): Stream[F, A] =
  (Stream.unit ++ Stream.fixedDelay(delay)) >> Stream.eval(fa)
