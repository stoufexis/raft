package com.stoufexis.leader.rpc

import com.stoufexis.leader.model.Term

enum VoteResponse:
  case Granted
  case Rejected(term: Term, reason: String)
