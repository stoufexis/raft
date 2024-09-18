package com.stoufexis.raft.persist

import cats.data.NonEmptySeq
import fs2.*

import com.stoufexis.raft.model.*

trait Log[F[_], In]:
  // Returns the new last index of the log
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
