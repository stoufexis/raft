package com.stoufexis.raft.kvstore.statemachine

import io.circe
import io.circe.Codec

import com.stoufexis.raft.kvstore.*

enum KvResponse derives CanEqual, Codec:
  case Success
  case Values(revisions: RevisionId, values: Map[String, String])
