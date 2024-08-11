package com.stoufexis.leader.rpc

import com.stoufexis.leader.model.*

case class VoteRequest(from: NodeId, term: Term)