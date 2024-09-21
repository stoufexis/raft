package com.stoufexis.raft.kvstore

import io.circe.*
import org.http4s.*

import com.stoufexis.raft.model.NodeId

extension (node: NodeId)
  def toUriInternal: Uri =
    Uri.unsafeFromString(s"http://${node.internalId}")

  def toUriExternal: Uri =
    Uri.unsafeFromString(s"http://${node.externalId}")

extension [A](a: A) def encode(using c: Encoder[A]): Json = c(a)
