package com.stoufexis.raft.rpc

import com.stoufexis.raft.model.*

case class AppendEntries[In](
  leaderId:     NodeId,
  term:         Term,
  prevLogIndex: Index,
  prevLogTerm:  Term,
  entries:      Seq[(Term, Command[In])]
)