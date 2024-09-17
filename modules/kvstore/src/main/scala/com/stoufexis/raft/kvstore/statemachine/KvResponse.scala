package com.stoufexis.raft.kvstore.statemachine

import io.circe
import io.circe.Json
import io.circe.syntax.given
import com.stoufexis.raft.kvstore.*

enum KvResponse derives CanEqual:
  case Success
  case Values(revisions: RevisionId, values: Map[String, String])

object KvResponse:
  given circe.Encoder[KvResponse] =
    case KvResponse.Success =>
      Json.obj("typ" -> "SUCCESS".asJson)

    case KvResponse.Values(r, vs) =>
      val ridString: String = r.encode.toBase64
      Json.obj("typ" -> "VALUES".asJson, "revision_id" -> ridString.asJson, "values" -> vs.asJson)