package com.stoufexis.raft.rpc

import fs2.Chunk

import com.stoufexis.raft.model.*

case class AppendEntries[In](
  leaderId:     NodeId,
  term:         Term,
  prevLogIndex: Index,
  prevLogTerm:  Term,
  entries:      Chunk[Command[In]]
)