package com.stoufexis.leader.rpc

import com.stoufexis.leader.model.*

case class RequestVote(from: NodeId, term: Term)