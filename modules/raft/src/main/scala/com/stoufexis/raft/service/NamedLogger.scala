package com.stoufexis.raft.service

import com.stoufexis.raft.model.NodeInfo
import org.typelevel.log4cats.Logger

trait NamedLogger[F[_]]:
  def fromState(state: NodeInfo[?]): F[Logger[F]]

object NamedLogger:
  def apply[F[_]: NamedLogger]: NamedLogger[F] = summon