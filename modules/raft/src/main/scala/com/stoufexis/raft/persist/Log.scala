package com.stoufexis.raft.persist

import cats.data.NonEmptySeq
import fs2.*

import com.stoufexis.raft.model.*

trait Log[F[_], In]:
  // None entry should simply advance the index. User for linearizable reads
  // Returns the new last index of the log
  // Throws if prevIdx is not the current last index, as this an illegal state
  // Returns none if the commandId already exists
  def append(term: Term, entries: NonEmptySeq[Command[In]]): F[Index]

  def commandIdExists(commandId: CommandId): F[Boolean]

  def matches(prevLogTerm: Term, prevLogIndex: Index): F[Boolean]

  def deleteAfter(index: Index): F[Unit]

  /** Inclusive range
    */
  def range(from: Index, until: Index): F[Seq[Command[In]]]

  def rangeStream(from: Index, until: Index): Stream[F, (Index, Command[In])]

  def lastTermIndex: F[Option[(Term, Index)]]

  def term(index: Index): F[Term]
