package com.stoufexis.leader.statemachine

import fs2.*

import com.stoufexis.leader.model.*

trait Log[F[_], A]:
  // empty chunk should advance the index. User for linearizable reads
  // prevIdx is the expected index of the currently last entry in the log
  // it overwrites all entries after prevIdx with the new entries
  def appendChunk(term: Term, prevIdx: Index, entries: Chunk[A]): F[Index]

  def appendChunkIfMatches(
    prevLogTerm:  Term,
    prevLogIndex: Index,
    term:         Term,
    entries:      Chunk[A]
  ): F[Boolean]

  /** Inclusive range
    */
  def range(from: Index, until: Index): F[(Term, Chunk[A])]

  def readAll: Stream[F, (Index, A)]

  def readRange(from: Index, until: Index): Stream[F, A]

  def term(index: Index): F[Term]
