package com.stoufexis.leader.rpc

import com.stoufexis.leader.model.*

enum HeartbeatResponse:
  case Accepted
  case TermExpired(newTerm: Term)
