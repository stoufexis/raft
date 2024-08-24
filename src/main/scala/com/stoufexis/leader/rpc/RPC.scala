package com.stoufexis.leader.rpc

import fs2.Stream

import com.stoufexis.leader.model.NodeId

import scala.concurrent.duration.FiniteDuration

/** Should handle retries. Should make sure that received messages are from nodes within the
  * cluster, so only known NodeIds.
  */
trait RPC[F[_], A]:
  def broadcastVotes(
    repeat:  FiniteDuration,
    request: RequestVote
  ): Stream[F, (NodeId, VoteResponse)]

  def broadcastAppends(
    repeat:  FiniteDuration,
    request: AppendEntries[A]
  ): Stream[F, (NodeId, AppendResponse)]

  def incomingVotes: Stream[F, IncomingVote[F]]

  def incomingAppends: Stream[F, IncomingAppend[F, A]]