package com.stoufexis.raft.rpc

import com.stoufexis.raft.model.NodeId

enum ClientResponse[+S] derives CanEqual:
  case Executed[S](state: S)      extends ClientResponse[S]
  case Redirect(leaderId: NodeId) extends ClientResponse[Nothing]
  case UnknownLeader              extends ClientResponse[Nothing]

object ClientResponse:
  def knownLeader(leaderId: Option[NodeId]): ClientResponse[Nothing] =
    leaderId match
      case None    => UnknownLeader
      case Some(n) => Redirect(n)
