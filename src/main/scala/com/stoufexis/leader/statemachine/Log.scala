package com.stoufexis.leader.statemachine

import com.stoufexis.leader.model.*
import fs2.*
import scala.concurrent.duration.FiniteDuration

trait Log[F[_], A]:
  def append(term: Term, entry: A): F[Index]

  def entriesAfter(index: Index): F[(Term, Chunk[A])]

  def term(index: Index): F[Term]

  def readAll: Stream[F, A]

trait LocalLog[F[_], A]:
  def commit(node: NodeId, idx: Index): F[Unit]

  /**
    * If there is no new index for advertiseEvery duration, repeat the previous index
    */
  def uncommitted(node: NodeId, advertiseEvery: FiniteDuration): Stream[F, Index]

  def majorityCommit(idx: Index): F[Unit]

  def commitIdx: F[Index]