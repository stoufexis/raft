package com.stoufexis.raft.model

opaque type CommandId = String

object CommandId:
  infix def apply(str: String): CommandId = str