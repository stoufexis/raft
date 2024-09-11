package com.stoufexis.raft.statemachine

import cats.effect.kernel.Sync
import cats.effect.std.Random
import cats.implicits.given

import scala.concurrent.duration.*

// TODO Rename this trait
trait ElectionTimeout[F[_]]:
  def nextElectionTimeout: F[FiniteDuration]

object ElectionTimeout:
  def fromRange[F[_]: Sync](from: FiniteDuration, to: FiniteDuration): F[ElectionTimeout[F]] =
    for
      rnd <- Random.scalaUtilRandom[F]
    yield new:
      def nextElectionTimeout: F[FiniteDuration] =
        rnd.betweenLong(from.toMillis, to.toMillis).map(_.millis)