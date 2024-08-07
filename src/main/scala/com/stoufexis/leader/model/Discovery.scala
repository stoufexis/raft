package com.stoufexis.leader.model

enum Discovery:
  case CurrentNode
  case Undetermined(reason: String)
  case Node(nodeId: NodeId)
