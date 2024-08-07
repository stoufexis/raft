package com.stoufexis.leader.rpc

import com.stoufexis.leader.model.Term

case class RequestVote[A](term: Term, entity: A)