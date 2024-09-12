package com.stoufexis.raft.model

case class CommandId(client: ClientId, serial: SerialNr)