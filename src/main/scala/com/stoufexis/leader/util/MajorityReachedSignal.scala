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
          Deferred[F, Unit].flatMap(confirms.offer)

      confirmLeader: ConfirmLeader[F] = new:
        def leaderConfirmed: F[Unit] =
          confirms.tryTake.flatMap:
            case None           => F.unit
            case Some(deferred) => deferred.complete_ >> leaderConfirmed
    yield (confirmLeader, confirmLeaderWait)
