package com.stoufexis.raft.model

case class Command[A](id: Option[CommandId], value: A)
