package com.stoufexis.raft

import com.stoufexis.raft.model.NodeId
import com.stoufexis.raft.rpc.*

trait ExternalNode[F[_], A, S]:
  val id: NodeId

  def appendEntries(req: AppendEntries[A]): F[AppendResponse]

  def requestVote(request: RequestVote): F[VoteResponse]
