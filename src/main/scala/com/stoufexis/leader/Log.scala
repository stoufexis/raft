package com.stoufexis.leader

import com.stoufexis.leader.model.*
import fs2.*

trait Log[F[_], A]:
  def entry(term: Term, entry: A): F[Index]

trait LocalLog[F[_], A]:
  def commit(node: NodeId, idx: Index): F[Unit]

  def uncommitted(node: NodeId): Stream[F, Index]

  def majorityCommit(idx: Index): F[Unit]