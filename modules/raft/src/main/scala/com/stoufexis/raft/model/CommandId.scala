package com.stoufexis.raft.model

opaque type CommandId = String

object CommandId:
  extension (n: CommandId)
    infix def string: String = n

  infix def apply(str: String): CommandId = str