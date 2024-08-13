package com.stoufexis.leader.util

import cats.effect.kernel.*
import cats.effect.std.Queue
import cats.implicits.*

trait ConfirmLeaderWait[F[_]]:
  def waitUntilConfirmed: F[Unit]

trait ConfirmLeader[F[_]]:
  def leaderConfirmed: F[Unit]

object ConfirmedLeader:
  def issue[F[_]](using F: Concurrent[F]): F[(ConfirmLeader[F], ConfirmLeaderWait[F])] =
    for
      confirms: Queue[F, Deferred[F, Unit]] <-
        Queue.unbounded

      confirmLeaderWait: ConfirmLeaderWait[F] = new:
        def waitUntilConfirmed: F[Unit] =
          for
            deferred <- Deferred[F, Unit]
            _        <- confirms.offer(deferred)
            _        <- deferred.get
          yield ()

      confirmLeader: ConfirmLeader[F] = new:
        def leaderConfirmed: F[Unit] =
          def go: F[Unit] =
            confirms.tryTake.flatMap:
              case None           => F.unit
              case Some(deferred) => deferred.complete_ >> go

          F.uncancelable(_ => go)
        end leaderConfirmed
    yield (confirmLeader, confirmLeaderWait)
