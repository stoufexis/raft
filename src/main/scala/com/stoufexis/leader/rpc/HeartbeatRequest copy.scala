package com.stoufexis.leader.rpc

import com.stoufexis.leader.model.*

case class HeartbeatRequest(from: NodeId, term: Term)
