package com.stoufexis.raft.model

case class Command[A](id: CommandId, value: A)