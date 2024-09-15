package com.stoufexis.raft.model

opaque type NodeId = String

object NodeId:
  extension (n: NodeId)
    infix def string: String = n

  inline def apply(str: String): NodeId = str

  given CanEqual[NodeId, NodeId] = CanEqual.derived
