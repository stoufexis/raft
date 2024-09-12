package com.stoufexis.raft.persist

import fs2.*

import com.stoufexis.raft.model.*

enum AppendFailure derives CanEqual:
  case CommandExists
  case SerialGap(previous: SerialNr)

trait Log[F[_], A]:
  // None entry should simply advance the index. User for linearizable reads
  // Returns the new last index of the log
  // Throws if prevIdx is not the current last index
  def append(
    term:    Term,
    prevIdx: Index,
    entry:   Option[Command[A]]
  ): F[Either[AppendFailure, Index]]

  def overwriteChunkIfMatches(
    prevLogTerm:  Term,
    prevLogIndex: Index,
    term:         Term,
    entries:      Chunk[Command[A]]
  ): F[Option[Index]]

  /** Inclusive range
    */
  def range(from: Index, until: Index): F[Chunk[Command[A]]]

  def rangeStream(from: Index, until: Index): Stream[F, (Index, Command[A])]

  def lastTermIndex: F[(Term, Index)]

  def term(index: Index): F[Term]
