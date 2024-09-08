package com.stoufexis.raft.statemachine

import com.stoufexis.raft.model.*

trait PersistedState[F[_]]:
  def persist(term: Term, vote: Option[NodeId]): F[Unit]

  def readLatest: F[(Term, Option[NodeId])]
