package com.stoufexis.raft.model

import doobie.util.*

opaque type NodeId = String

object NodeId:
  inline def unsafe(str: String): NodeId = str

  given CanEqual[NodeId, NodeId] = CanEqual.derived
  given Get[NodeId]              = Get[String]
  given Put[NodeId]              = Put[String]
