package com.stoufexis.raft.persist

import com.stoufexis.raft.model.*

trait PersistedState[F[_]]:
  def persist(term: Term, vote: Option[NodeId]): F[Unit]

  def readLatest: F[Option[(Term, Option[NodeId])]]

object PersistedState:
  def apply[F[_]: PersistedState]: PersistedState[F] = summon