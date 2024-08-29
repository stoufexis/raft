package com.stoufexis.leader.model

opaque type NodeId = String

object NodeId:
  inline def unsafe(str: String): NodeId = str

  given CanEqual[NodeId, NodeId] = CanEqual.derived