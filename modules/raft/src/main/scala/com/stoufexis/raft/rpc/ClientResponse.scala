package com.stoufexis.raft.rpc

import com.stoufexis.raft.model.NodeId

enum ClientResponse[+Out, +S] derives CanEqual:
  case Executed[Out, S](state: S, output: Out) extends ClientResponse[Out, S]
  case Skipped[S](state: S)                    extends ClientResponse[Nothing, S]
  case Redirect(leaderId: NodeId)              extends ClientResponse[Nothing, Nothing]
  case UnknownLeader                           extends ClientResponse[Nothing, Nothing]

object ClientResponse:
  def knownLeader(leaderId: Option[NodeId]): ClientResponse[Nothing, Nothing] =
    leaderId match
      case None    => UnknownLeader
      case Some(n) => Redirect(n)
