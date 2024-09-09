package com.stoufexis.impl

import cats.effect.*
import com.stoufexis.impl.proto.moreprotos.PMessage
import io.grpc.Metadata

import com.stoufexis.raft.StructerServiceImpl
import com.stoufexis.raft.proto.protos.MessageIn

object Server extends IOApp.Simple:
  override def run: IO[Unit] =
    StructerServiceImpl.runServer[PMessage].useForever

object Client extends IOApp.Simple:
  override def run: IO[Unit] =
    StructerServiceImpl.client.use: cl =>
      cl.astruct(
        MessageIn(Seq(PMessage("1"), PMessage("2")).map(com.google.protobuf.any.Any.pack)),
        Metadata()
      ).flatMap(IO.println)
