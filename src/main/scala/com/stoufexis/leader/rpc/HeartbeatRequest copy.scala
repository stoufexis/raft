package com.stoufexis.leader.rpc

import com.stoufexis.leader.model.*

case class HeartbeatRequest[A](from: NodeId, term: Term, entity: A)
