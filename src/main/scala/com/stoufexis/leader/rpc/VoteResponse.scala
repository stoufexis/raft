package com.stoufexis.leader.rpc

import com.stoufexis.leader.model.Term

enum VoteResponse:
  case Granted
  case Rejected
  case TermExpired(term: Term)
  case IllegalState(state: String)
