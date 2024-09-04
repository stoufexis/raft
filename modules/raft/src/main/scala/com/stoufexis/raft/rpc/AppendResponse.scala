package com.stoufexis.raft.rpc

import com.stoufexis.raft.model.*

enum AppendResponse derives CanEqual:
  case Accepted
  case NotConsistent
  case TermExpired(newTerm: Term)
  case IllegalState(state: String)
