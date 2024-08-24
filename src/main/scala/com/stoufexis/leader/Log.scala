package com.stoufexis.leader

import com.stoufexis.leader.model.*

trait Log[F[_], A]:
  def entry(term: Term, entry: A): F[Index]

  def commit(seqNr: Index): F[Unit]
