package com.stoufexis.raft.rpc

import com.stoufexis.raft.model.NodeId
import com.stoufexis.raft.model.SerialNr

enum ClientResponse[+S] derives CanEqual:
  case Executed[S](state: S)         extends ClientResponse[S]
  case Skipped[S](state: S)          extends ClientResponse[S]
  case SerialGap(previous: SerialNr) extends ClientResponse[Nothing]
  case Redirect(leaderId: NodeId)    extends ClientResponse[Nothing]
  case UnknownLeader                 extends ClientResponse[Nothing]

object ClientResponse:
  def knownLeader(leaderId: Option[NodeId]): ClientResponse[Nothing] =
    leaderId match
      case None    => UnknownLeader
      case Some(n) => Redirect(n)
