package com.stoufexis.leader.rpc

import fs2.Chunk

import com.stoufexis.leader.model.*

case class AppendEntries[A](
  leaderId:     NodeId,
  term:         Term,
  prevLogIndex: Index,
  prevLogTerm:  Term,
  entries:      Chunk[A]
)