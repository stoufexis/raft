package com.stoufexis.raft.kvstore.rpc

import cats.effect.*
import org.http4s.client.Client
import org.http4s.client.middleware.*
import org.http4s.{Method, Request}

import com.stoufexis.raft.ExternalNode
import com.stoufexis.raft.kvstore.*
import com.stoufexis.raft.kvstore.implicits.given
import com.stoufexis.raft.kvstore.statemachine.KvCommand
import com.stoufexis.raft.model.*
import com.stoufexis.raft.rpc.*

import scala.concurrent.duration.FiniteDuration
import org.http4s.Uri

object RpcClient:
  def apply[F[_]: Temporal](
    nodeId:     NodeId,
    retryAfter: FiniteDuration,
    client:     Client[F]
  ): ExternalNode[F, KvCommand] =
    new:
      val id: NodeId = nodeId

      val uri: Uri = nodeId.toUri

      val retryingClient: Client[F] =
        Retry(RetryPolicy(_ => Some(retryAfter)))(client)

      def appendEntries(req: AppendEntries[KvCommand]): F[AppendResponse] =
        retryingClient.expect(Request(Method.PUT, uri / "raft" / "append_entries").withEntity(req))

      def requestVote(req: RequestVote): F[VoteResponse] =
        retryingClient.expect(Request(Method.PUT, uri / "raft" / "request_vote").withEntity(req))
