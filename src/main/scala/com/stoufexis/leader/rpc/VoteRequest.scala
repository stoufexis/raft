package com.stoufexis.leader.rpc

import com.stoufexis.leader.model.*

case class VoteRequest[A](from: NodeId, term: Term, entity: A)