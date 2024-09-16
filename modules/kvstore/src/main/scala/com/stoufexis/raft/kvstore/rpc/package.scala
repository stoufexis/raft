package com.stoufexis.raft.kvstore.rpc

import org.http4s.Uri
import org.http4s.websocket.WebSocketFrame
import scodec.*
import scodec.bits.ByteVector

import com.stoufexis.raft.model.NodeId

case object EmptyValueReceived extends RuntimeException("Received empty value")

case class WebsocketResponse[A](either: Either[String, A])

object WebsocketResponse:
  def apply[A](value: A): WebsocketResponse[A] = WebsocketResponse(Right(value))

  def failed(str: String): WebsocketResponse[Nothing] = WebsocketResponse(Left(str))

  given [A: Codec]: Codec[WebsocketResponse[A]] =
    Codec[(Option[String], Option[A])].exmap(
      {
        case (_, Some(a))   => Attempt.Successful(WebsocketResponse(Right(a)))
        case (Some(str), _) => Attempt.Successful(WebsocketResponse(Left(str)))
        case _              => Attempt.Failure(Err("unreachable"))
      },
      {
        case WebsocketResponse(Right(a))  => Attempt.Successful(None, Some(a))
        case WebsocketResponse(Left(str)) => Attempt.Successful(Some(str), None)
      }
    )

extension [A](res: A)
  def toFrame(using codec: Codec[A]): WebSocketFrame =
    WebSocketFrame.Binary(codec.encode(res).require.bytes)

  def encode(using codec: Codec[A]): ByteVector =
    codec.encode(res).require.bytes

extension (nodeId: NodeId)
  def toUri: Uri = ???
