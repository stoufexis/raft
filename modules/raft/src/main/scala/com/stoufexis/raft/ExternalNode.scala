package com.stoufexis.raft

import com.stoufexis.raft.model.NodeId
import com.stoufexis.raft.rpc.*

/** Implementations should handle not fatal errors by indefinitely retrying, with appropriate delays or
  * backoff between attempts.
  */
trait ExternalNode[F[_], In]:
  val id: NodeId

  def appendEntries(req: AppendEntries[In]): F[AppendResponse]

  def requestVote(request: RequestVote): F[VoteResponse]
