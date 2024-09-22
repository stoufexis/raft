package com.stoufexis.raft.kvstore.statemachine

import io.circe.*
import io.circe.syntax.*

import com.stoufexis.raft.kvstore.*

enum KvResponse derives CanEqual:
  case Success
  case Values(revisions: RevisionId, values: Map[String, String])

object KvResponse:
  given Encoder[KvResponse] =
    case KvResponse.Success =>
      Json.obj("status" -> "SUCCESS".asJson)

    case KvResponse.Values(rid, vs) =>
      Json.obj("status" -> "VALUES".asJson, "revision_id" -> rid.asJson, "values" -> vs.asJson)
