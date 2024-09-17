package com.stoufexis.raft.kvstore

import io.circe.*
import org.http4s.Uri

import com.stoufexis.raft.model.NodeId

extension (node: NodeId) def toUri: Uri = ???

extension [A](a:      A)
  def encode(using c: Encoder[A]): Json = c(a)
