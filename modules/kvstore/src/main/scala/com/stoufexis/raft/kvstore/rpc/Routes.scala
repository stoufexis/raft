package com.stoufexis.raft.kvstore.rpc

import cats.*
import cats.data.*
import cats.implicits.given
import io.circe
import io.circe.Json
import io.circe.syntax.given
import org.http4s.*
import org.http4s.circe.given
import org.http4s.dsl.*
import org.http4s.headers.Location
import org.http4s.server.websocket.WebSocketBuilder2
import org.http4s.websocket.*
import org.typelevel.log4cats.Logger
import scodec.*
import scodec.Attempt.Failure
import scodec.Attempt.Successful
import scodec.bits.*

import com.stoufexis.raft.RaftNode
import com.stoufexis.raft.kvstore.implicits.given
import com.stoufexis.raft.kvstore.statemachine.*
import com.stoufexis.raft.model.Command
import com.stoufexis.raft.model.CommandId
import com.stoufexis.raft.rpc.*

class RaftRoutes[F[_]](raft: RaftNode[F, KvCommand, KvResponse, KvState], wsb: WebSocketBuilder2[F])(using
  F:   MonadThrow[F],
  log: Logger[F]
) extends Http4sDsl[F]:

  object AppendFrame:
    def unapply(data: ByteVector): Option[AppendEntries[KvCommand]] =
      Codec[AppendEntries[KvCommand]].decodeValue(data.bits).toOption

  object VoteFrame:
    def unapply(data: ByteVector): Option[RequestVote] =
      Codec[RequestVote].decodeValue(data.bits).toOption

  def make: HttpRoutes[F] = HttpRoutes.of:
    case GET -> Root / "raft" / "append_entries" =>
      val process: WebSocketFrame => F[WebSocketFrame] =
        case WebSocketFrame.Binary(AppendFrame(command), _) =>
          raft.appendEntries(command).map(WebsocketResponse(_).toFrame)

        case WebSocketFrame.Binary(_, _) =>
          F.pure(WebsocketResponse.failed("Deserialization failure").toFrame)

        case WebSocketFrame.Ping(d) =>
          F.pure(WebSocketFrame.Pong(d))

        case WebSocketFrame.Close(d) =>
          F.pure(WebSocketFrame.Close(d))

        case fr =>
          log.info(s"Received unsupported frame $fr") as WebSocketFrame.Close()

      wsb.build(_.evalMap(process))

    case GET -> Root / "raft" / "request_vote" =>
      val process: WebSocketFrame => F[WebSocketFrame] =
        case WebSocketFrame.Binary(VoteFrame(vote), _) =>
          raft.requestVote(vote).map(WebsocketResponse(_).toFrame)

        case WebSocketFrame.Binary(_, _) =>
          F.pure(WebsocketResponse.failed("Deserialization failure").toFrame)

        case WebSocketFrame.Ping(d) =>
          F.pure(WebSocketFrame.Pong(d))

        case WebSocketFrame.Close(d) =>
          F.pure(WebSocketFrame.Close(d))

        case fr =>
          log.info(s"Received unsupported frame $fr") as WebSocketFrame.Close()

      wsb.build(_.evalMap(process))

class ClientRoutes[F[_]](raft: RaftNode[F, KvCommand, KvResponse, KvState])(using
  F:   MonadThrow[F],
  log: Logger[F]
) extends Http4sDsl[F]:

  object GetKeysParam:
    def unapply(params: Map[String, collection.Seq[String]]): Option[Set[String]] =
      params.get("keys").map(_.toSet)

  object UpdateKeysParam:
    def unapply(params: Map[String, collection.Seq[String]]): Option[Map[String, Option[String]]] =
      ???

  case object CidQuery extends QueryParamDecoderMatcher[CommandId]("command_id")

  case object RidQuery extends QueryParamDecoderMatcher[RevisionId]("revision_id")

  def foldResponse(cr: ClientResponse[KvResponse, KvState]): F[Response[F]] =
    cr match
      case ClientResponse.Executed(_, output) => Ok((output: KvResponse).asJson)
      case ClientResponse.Skipped(_)          => NotModified()
      case ClientResponse.Redirect(leaderId)  => TemporaryRedirect(Location(leaderId.toUri))
      case ClientResponse.UnknownLeader()     => TemporaryRedirect()
  
  def make: HttpRoutes[F] = HttpRoutes.of:
    case GET -> Root / "store" :? CidQuery(cid) +& GetKeysParam(keys) =>
      raft
        .clientRequest(Command(cid, KvCommand.Get(keys)))
        .flatMap(foldResponse)

    case PUT -> Root / "store" :? CidQuery(cid) +& UpdateKeysParam(keys) =>
      raft
        .clientRequest(Command(cid, KvCommand.Update(keys)))
        .flatMap(foldResponse)

    case PUT -> Root / "store" / "tx" :? CidQuery(cid) +& RidQuery(rid) +& UpdateKeysParam(keys) =>
      raft
        .clientRequest(Command(cid, KvCommand.TransactionUpdate(rid, keys)))
        .flatMap(foldResponse)
