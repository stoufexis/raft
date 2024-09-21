package com.stoufexis.raft.model

case class NodeId(internalId: String, externalId: String) derives CanEqual