package com.stoufexis.raft

import com.stoufexis.raft.model.NodeId
import com.stoufexis.raft.rpc.*

trait ExternalNode[F[_], In]:
  val id: NodeId

  def appendEntries(req: AppendEntries[In]): F[AppendResponse]

  def requestVote(request: RequestVote): F[VoteResponse]
