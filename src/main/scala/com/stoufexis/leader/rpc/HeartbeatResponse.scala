package com.stoufexis.leader.rpc

import com.stoufexis.leader.model.*

enum HeartbeatResponse derives CanEqual:
  case Accepted
  case TermExpired(newTerm: Term)
