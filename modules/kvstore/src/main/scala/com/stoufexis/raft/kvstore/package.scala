package com.stoufexis.raft.kvstore

import io.circe.*
import org.http4s.*

import com.stoufexis.raft.model.NodeId

extension (node: NodeId)
  def toUri: Uri =
    Uri.unsafeFromString(s"http://${node.string}")

extension [A](a: A) def encode(using c: Encoder[A]): Json = c(a)
