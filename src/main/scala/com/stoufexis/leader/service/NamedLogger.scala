package com.stoufexis.leader.service

import com.stoufexis.leader.model.NodeInfo
import org.typelevel.log4cats.Logger

trait NamedLogger[F[_]]:
  def fromState(state: NodeInfo[?]): F[Logger[F]]

object NamedLogger:
  def apply[F[_]: NamedLogger]: NamedLogger[F] = summon