package com.stoufexis.leader.model

enum Discovery derives CanEqual:
  case CurrentNode
  case Undetermined(reason: String)
  case Node(nodeId: NodeId)
