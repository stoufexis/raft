package com.stoufexis.raft.statemachine

import cats.effect.kernel.Sync
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jFactory

import cats.implicits.given

trait NamedLogger[F[_]]:
  def fromState(state: NodeInfo): F[Logger[F]]

object NamedLogger:
  def apply[F[_]: NamedLogger]: NamedLogger[F] = summon

  given [F[_]: Sync]: NamedLogger[F] with
    def fromState(state: NodeInfo): F[Logger[F]] =
      Slf4jFactory.create[F].fromName(state.print).widen
