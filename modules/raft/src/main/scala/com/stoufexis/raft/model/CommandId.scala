package com.stoufexis.raft.model

import doobie.util.*

opaque type CommandId = String

object CommandId:
  extension (n: CommandId)
    infix def string: String = n

  infix def apply(str: String): CommandId = str

  given Put[CommandId] = Put[String]
  given Get[CommandId] = Get[String]
