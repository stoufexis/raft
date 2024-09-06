package com.stoufexis.raft.rpc

import fs2.Stream

import com.stoufexis.raft.model.NodeId

/** Should handle retries. Should make sure that received messages are from nodes within the
  * cluster, so only known NodeIds.
  */
trait RPC[F[_], A, S]:
  // Repeats on transient errors
  def appendEntries(node: NodeId, request: AppendEntries[A]): F[AppendResponse]

  // Repeats on transient errors
  def requestVote(node: NodeId, request: RequestVote): F[VoteResponse]

  def incomingVotes: Stream[F, IncomingVote[F]]

  def incomingAppends: Stream[F, IncomingAppend[F, A]]

  def incomingClientRequests: Stream[F, IncomingClientRequest[F, A, S]]