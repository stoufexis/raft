package com.stoufexis.leader.rpc

import com.stoufexis.leader.model.*

enum HeartbeatResponse:
  case Accepted
  case Rejected(term: Term, reason: String)
