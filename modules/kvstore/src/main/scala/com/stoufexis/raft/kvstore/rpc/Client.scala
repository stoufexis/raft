package com.stoufexis.raft.kvstore.rpc

import cats.MonadThrow

import com.stoufexis.raft.ExternalNode
import com.stoufexis.raft.kvstore.statemachine.KvCommand
import com.stoufexis.raft.model.*
import com.stoufexis.raft.rpc.*

object Client:
  def externalNode[F[_]](nodeId: NodeId)(using
    F: MonadThrow[F]
  ): ExternalNode[F, KvCommand] =
    new:
      override val id: NodeId = ???

      override def appendEntries(req: AppendEntries[KvCommand]): F[AppendResponse] = ???

      override def requestVote(request: RequestVote): F[VoteResponse] = ???
