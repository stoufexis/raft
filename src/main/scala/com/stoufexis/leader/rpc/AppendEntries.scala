package com.stoufexis.leader.rpc

import com.stoufexis.leader.model.*

case class AppendEntries(from: NodeId, term: Term)
