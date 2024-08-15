package com.stoufexis.leader.statemachine

import cats.effect.kernel.*
import cats.implicits.given
import fs2.concurrent.*

class ResettableCountDownLatch[F[_]](using F: Concurrent[F]):
  def await: F[Boolean] = ???

  def nack: F[Unit] = ???

  def scopedAck: Resource[F, F[Unit]] = ???

object ResettableCountDownLatch:
  def apply[F[_]: Concurrent]: F[ResettableCountDownLatch[F]] =
    ???