package com.stoufexis.leader.rpc

import com.stoufexis.leader.model.Term

enum VoteResponse:
  case Granted
  case TermExpired(term: Term)
