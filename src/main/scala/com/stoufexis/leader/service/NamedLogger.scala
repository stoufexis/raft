package com.stoufexis.leader.service

import com.stoufexis.leader.model.NodeState
import org.typelevel.log4cats.Logger

trait NamedLogger[F[_]]:
  def fromState(state: NodeState): F[Logger[F]]

object NamedLogger:
  def apply[F[_]: NamedLogger]: NamedLogger[F] = summon