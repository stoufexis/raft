package com.stoufexis.raft.persist

import fs2.*

import com.stoufexis.raft.model.*

trait Log[F[_], A]:
  // empty chunk should advance the index. User for linearizable reads
  // prevIdx is the expected index of the currently last entry in the log
  // it overwrites all entries after prevIdx with the new entries
  // Returns the new last index of the log
  def appendChunk(term: Term, prevIdx: Index, entries: Chunk[A]): F[Index]

  def appendChunkIfMatches(
    prevLogTerm:  Term,
    prevLogIndex: Index,
    term:         Term,
    entries:      Chunk[A]
  ): F[Option[Index]]

  /** Inclusive range. Returns the term from the first entry in the Chunk
    */
  def range(from: Index, until: Index): F[(Term, Chunk[A])]

  def readUntil(until: Index): Stream[F, (Index, A)]

  def lastTermIndex: F[(Term, Index)]

  def term(index: Index): F[Term]
