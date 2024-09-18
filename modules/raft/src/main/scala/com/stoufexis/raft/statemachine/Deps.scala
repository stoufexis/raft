package com.stoufexis.raft.statemachine

import com.stoufexis.raft.persist.*
import com.stoufexis.raft.rpc.*
import com.stoufexis.raft.statemachine.*

import scala.concurrent.duration.FiniteDuration

case class Deps[F[_], In, Out, S](
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
given [F[_], In](using cfg: Deps[F, In, ?, ?]): Log[F, In] = cfg.log

given [F[_]](using cfg: Deps[F, ?, ?, ?]): PersistedState[F] = cfg.persisted

given [F[_], In](using cfg: Deps[F, In, ?, ?]): Cluster[F, In] = cfg.cluster

given [F[_]](using cfg: Deps[F, ?, ?, ?]): ElectionTimeout[F] = cfg.timeout

given [F[_], In, Out, S](using cfg: Deps[F, In, Out, S]): InputSource[F, In, Out, S] = cfg.inputs

given [F[_]](using in: InputSource[F, ?, ?, ?]): InputVotes[F] = in

given [F[_], In](using in: InputSource[F, In, ?, ?]): InputAppends[F, In] = in

given [F[_], In, Out, S](using in: InputSource[F, In, Out, S]): InputClients[F, In, Out, S] = in

given (using in: Deps[?, ?, ?, ?]): AppenderCfg =
  AppenderCfg(in.appenderBatchSize, in.heartbeatEvery)

given [In, Out, S](using in: Deps[?, In, Out, S]): Automaton[In, Out, S] = Automaton(in.automaton)
