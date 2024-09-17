package com.stoufexis.raft.kvstore

import org.http4s.Uri
import scodec.Codec
import scodec.bits.*

import com.stoufexis.raft.model.NodeId

extension (node: NodeId) def toUri: Uri = ???

extension [A](a: A)
  def encode(using c: Codec[A]): ByteVector =
    c.encode(a).require.bytes
