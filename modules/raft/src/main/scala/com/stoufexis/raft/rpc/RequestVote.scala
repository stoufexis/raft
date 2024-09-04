package com.stoufexis.raft.rpc

import com.stoufexis.raft.model.*

case class RequestVote(candidateId: NodeId, term: Term)