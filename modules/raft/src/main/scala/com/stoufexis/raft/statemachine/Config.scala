package com.stoufexis.raft.statemachine

import com.stoufexis.raft.persist.*
import com.stoufexis.raft.rpc.*
import com.stoufexis.raft.statemachine.*

import scala.concurrent.duration.FiniteDuration

case class Config[F[_], In, Out, S](
  automaton:         (S, In) => (S, Out),
  log:               Log[F, In],
  persisted:         PersistedState[F],
  cluster:           Cluster[F, In],
  heartbeatEvery:    FiniteDuration,
  timeout:           ElectionTimeout[F],
  inputs:            InputSource[F, In, Out, S],
  appenderBatchSize: Int
)

// toplevel here so they are available in the package
given [F[_], In, Out, S](using cfg: Config[F, In, Out, S]): Log[F, In]                 = cfg.log
given [F[_], In, Out, S](using cfg: Config[F, In, Out, S]): PersistedState[F]          = cfg.persisted
given [F[_], In, Out, S](using cfg: Config[F, In, Out, S]): Cluster[F, In]             = cfg.cluster
given [F[_], In, Out, S](using cfg: Config[F, In, Out, S]): ElectionTimeout[F]         = cfg.timeout
given [F[_], In, Out, S](using cfg: Config[F, In, Out, S]): InputSource[F, In, Out, S] = cfg.inputs
