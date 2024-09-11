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
  timeout:           Timeout[F],
  inputs:            InputSource[F, A, S],
  appenderBatchSize: Int
)
