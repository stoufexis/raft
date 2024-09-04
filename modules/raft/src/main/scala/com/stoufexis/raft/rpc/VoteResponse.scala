package com.stoufexis.raft.rpc

import com.stoufexis.raft.model.Term

enum VoteResponse derives CanEqual:
  case Granted
  case Rejected
  case TermExpired(term: Term)
  case IllegalState(state: String)
