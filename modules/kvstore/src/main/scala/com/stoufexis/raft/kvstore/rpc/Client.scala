package com.stoufexis.raft.kvstore.rpc

import cats.effect.Concurrent
import org.http4s.client.Client
import org.http4s.{Method, Request}

import com.stoufexis.raft.ExternalNode
import com.stoufexis.raft.kvstore.implicits.given
import com.stoufexis.raft.kvstore.statemachine.KvCommand
import com.stoufexis.raft.model.*
import com.stoufexis.raft.rpc.*
import com.stoufexis.raft.kvstore.*

object Clients:
  def externalNode[F[_]](nodeId: NodeId, client: Client[F])(using
    F: Concurrent[F]
  ): ExternalNode[F, KvCommand] =
    new:
      val id: NodeId = nodeId

      def appendEntries(req: AppendEntries[KvCommand]): F[AppendResponse] =
        client.expect(Request[F](Method.PUT, nodeId.toUri / "raft" / "append_entries").withEntity(req))

      def requestVote(req: RequestVote): F[VoteResponse] =
        client.expect(Request[F](Method.PUT, nodeId.toUri / "raft" / "request_vote").withEntity(req))
