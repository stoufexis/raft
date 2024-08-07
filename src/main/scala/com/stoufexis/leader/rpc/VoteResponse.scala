package com.stoufexis.leader.rpc

import com.stoufexis.leader.model.Term

enum VoteResponse:
  case Granted(term: Term)
  case Rejected(term: Term, reason: String)
