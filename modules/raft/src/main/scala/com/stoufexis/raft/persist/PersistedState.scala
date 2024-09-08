package com.stoufexis.raft.persist

import com.stoufexis.raft.model.*

trait PersistedState[F[_]]:
  def persist(term: Term, vote: Option[NodeId]): F[Unit]

  def readLatest: F[(Term, Option[NodeId])]
