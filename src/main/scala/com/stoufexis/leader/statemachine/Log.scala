package com.stoufexis.leader.statemachine

import com.stoufexis.leader.model.*
import fs2.*

trait Log[F[_], A]:
  // empty chunk should advance the index. User for linearizable reads
  def appendChunk(term: Term, entry: Chunk[A]): F[(Index, Index)]

  def range(from: Index, until: Index): F[(Term, Chunk[A])]

  def readAll: Stream[F, (Index, A)]

  def readRange(from: Index, until: Index): Stream[F, A]

  def term(index: Index): F[Term]