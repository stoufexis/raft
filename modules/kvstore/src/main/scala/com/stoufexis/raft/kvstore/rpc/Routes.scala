package com.stoufexis.raft.kvstore.rpc

import cats.*
import cats.effect.*
import cats.implicits.given
import io.circe
import io.circe.syntax.given
import org.http4s.*
import org.http4s.Uri.Authority
import org.http4s.circe.given
import org.http4s.dsl.*
import org.http4s.headers.Location

import com.stoufexis.raft.RaftNode
import com.stoufexis.raft.kvstore.*
import com.stoufexis.raft.kvstore.implicits.given
import com.stoufexis.raft.kvstore.statemachine.*
import com.stoufexis.raft.model.*
import com.stoufexis.raft.rpc.*

object Routes:
  def apply[F[_]: Concurrent](raft: RaftNode[F, KvCommand, KvResponse, KvState]): HttpRoutes[F] =
    object dsl extends Http4sDsl[F]
    import dsl.*

    object GetKeys:
      def unapply(params: Map[String, collection.Seq[String]]): Option[Set[String]] =
        params.get("keys").map(_.toSet)

    object UpdateKeys:
      def unapply(params: Map[String, collection.Seq[String]]): Option[Map[String, Option[String]]] =
        // I think .last is safe here
        Some(params.fmap(_.last).fmap(Some(_).filter(_.nonEmpty))).filter(_.nonEmpty)

    case object Cid extends QueryParamDecoderMatcher[CommandId]("command_id")

    case object Rid extends QueryParamDecoderMatcher[RevisionId]("revision_id")

    def foldResponse(req: Request[F], cr: ClientResponse[KvResponse, KvState]): F[Response[F]] =
      val currentLocation: Location =
        Location(req.uri.copy(authority = raft.id.toUri.authority))

      cr match
        case ClientResponse.Executed(_, output) =>
          Ok((output: KvResponse).asJson).map(_.withHeaders(currentLocation))

        case ClientResponse.Skipped(_) =>
          Conflict().map(_.withHeaders(currentLocation))

        case ClientResponse.UnknownLeader() =>
          TemporaryRedirect().map(_.withHeaders(currentLocation))

        case ClientResponse.Redirect(leaderId) =>
          TemporaryRedirect(Location(req.uri.copy(authority = leaderId.toUri.authority)))

    HttpRoutes.of:
      // internal
      case req @ PUT -> Root / "raft" / "append_entries" =>
        for
          ae  <- req.as[AppendEntries[KvCommand]]
          ar  <- raft.appendEntries(ae)
          res <- Ok()
        yield res.withEntity(ar)

      case req @ PUT -> Root / "raft" / "request_vote" =>
        for
          rv  <- req.as[RequestVote]
          vr  <- raft.requestVote(rv)
          res <- Ok()
        yield res.withEntity(vr)

      // client
      case req @ GET -> Root / "store" :? Cid(cid) +& GetKeys(keys) =>
        raft
          .clientRequest(Command(cid, KvCommand.Get(keys)))
          .flatMap(foldResponse(req, _))

      case req @ PUT -> Root / "store" :? Cid(cid) +& UpdateKeys(keys) =>
        raft
          .clientRequest(Command(cid, KvCommand.Update(keys)))
          .flatMap(foldResponse(req, _))

      case req @ PUT -> Root / "store" / "tx" :? Cid(cid) +& Rid(rid) +& UpdateKeys(keys) =>
        raft
          .clientRequest(Command(cid, KvCommand.TransactionUpdate(rid, keys)))
          .flatMap(foldResponse(req, _))