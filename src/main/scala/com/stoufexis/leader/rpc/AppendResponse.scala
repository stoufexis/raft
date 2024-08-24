package com.stoufexis.leader.rpc

import com.stoufexis.leader.model.*

enum AppendResponse derives CanEqual:
  case Accepted
  case NotConsistent
  case TermExpired(newTerm: Term)
  case IllegalState(state: String)
