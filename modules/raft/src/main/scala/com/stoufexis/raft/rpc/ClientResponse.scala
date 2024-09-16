package com.stoufexis.raft.rpc

import com.stoufexis.raft.model.NodeId

enum ClientResponse[+Out, +S] derives CanEqual:
  case Executed[Out, S](state: S, output: Out) extends ClientResponse[Out, S]
  case Skipped[Out, S](state: S)               extends ClientResponse[Out, S]
  case Redirect[Out, S](leaderId: NodeId)      extends ClientResponse[Out, S]
  case UnknownLeader[Out, S]()                 extends ClientResponse[Out, S]

object ClientResponse:
  def knownLeader(leaderId: Option[NodeId]): ClientResponse[Nothing, Nothing] =
    leaderId match
      case None    => UnknownLeader()
      case Some(n) => Redirect(n)
