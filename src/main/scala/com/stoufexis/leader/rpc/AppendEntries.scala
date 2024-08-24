package com.stoufexis.leader.rpc

import com.stoufexis.leader.model.*
import fs2.Chunk

case class AppendEntries[A](from: NodeId, term: Term, lastIndex: Index, lastTerm: Term, entries: Chunk[A])