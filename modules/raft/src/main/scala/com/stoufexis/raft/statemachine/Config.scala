package com.stoufexis.raft.statemachine

import com.stoufexis.raft.persist.*
import com.stoufexis.raft.rpc.*
import com.stoufexis.raft.statemachine.*

import scala.concurrent.duration.FiniteDuration

case class Config[F[_], A, S](
  automaton:         (S, A) => S,
  log:               Log[F, A],
  persisted:         PersistedState[F],
  cluster:           Cluster[F, A, S],
  heartbeatEvery:    FiniteDuration,
  timeout:           ElectionTimeout[F],
  inputs:            InputSource[F, A, S],
  appenderBatchSize: Int
)

// toplevel here so they are available in the package
given [F[_], A, S](using cfg: Config[F, A, S]): Log[F, A]            = cfg.log
given [F[_], A, S](using cfg: Config[F, A, S]): PersistedState[F]    = cfg.persisted
given [F[_], A, S](using cfg: Config[F, A, S]): Cluster[F, A, S]     = cfg.cluster
given [F[_], A, S](using cfg: Config[F, A, S]): ElectionTimeout[F]   = cfg.timeout
given [F[_], A, S](using cfg: Config[F, A, S]): InputSource[F, A, S] = cfg.inputs
