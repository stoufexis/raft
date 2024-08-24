package com.stoufexis.leader.rpc

import com.stoufexis.leader.model.*

case class RequestVote(candidateId: NodeId, term: Term)