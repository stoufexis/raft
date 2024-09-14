package com.stoufexis.raft.persist

import fs2.*

import com.stoufexis.raft.model.*

trait Log[F[_], In]:
  // None entry should simply advance the index. User for linearizable reads
  // Returns the new last index of the log
  // Throws if prevIdx is not the current last index, as this an illegal state
  // Returns none if the commandId already exists
  def append(
    term:    Term,
    prevIdx: Index,
    entry:   Command[In]
  ): F[Option[Index]]

  def overwriteChunkIfMatches(
    prevLogTerm:  Term,
    prevLogIndex: Index,
    term:         Term,
    entries:      Chunk[Command[In]]
  ): F[Option[Index]]

  /** Inclusive range
    */
  def range(from: Index, until: Index): F[Chunk[Command[In]]]

  def rangeStream(from: Index, until: Index): Stream[F, (Index, Command[In])]

  def lastTermIndex: F[(Term, Index)]

  def term(index: Index): F[Term]
