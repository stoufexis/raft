package com.stoufexis.leader.rpc

import com.stoufexis.leader.model.Term

enum VoteResponse[A]:
  case Granted[A](term: Term)                  extends VoteResponse[A]
  case Rejected[A](term: Term, reason: String) extends VoteResponse[A]
