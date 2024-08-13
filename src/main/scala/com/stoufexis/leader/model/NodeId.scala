package com.stoufexis.leader.model

opaque type NodeId = String

object NodeId:
  given CanEqual[NodeId, NodeId] = CanEqual.derived