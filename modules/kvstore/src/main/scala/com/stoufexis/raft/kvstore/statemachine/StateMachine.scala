package com.stoufexis.raft.kvstore.statemachine

import cats.implicits.given
import io.circe
import io.circe.Json
import io.circe.syntax.given
import scodec.Codec

import com.stoufexis.raft.kvstore.implicits.given
import com.stoufexis.raft.kvstore.rpc.*

case class RevisionId(revisions: Map[String, Long]) derives CanEqual, Codec

enum KvCommand derives CanEqual, Codec:
  case Get(keys: Set[String])
  case Update(sets: Map[String, Option[String]])
  case TransactionUpdate(revisions: RevisionId, sets: Map[String, Option[String]])

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

case class KvState(map: Map[String, (String, Long)]):
  def setAll(sets: Map[String, Option[String]]): KvState = KvState:
    sets.foldLeft(map): (acc, set) =>
      acc.updatedWith(set._1):
        case Some((_, r)) => set._2.map((_, r + 1))
        case None         => set._2.map((_, 0))

  def revisionMatches(rid: RevisionId): Boolean =
    rid.revisions.forall((k, r) => map.get(k).exists(_._2 == r))

  def getAll(keys: Set[String]): Map[String, (String, Long)] =
    keys.foldLeft(Map.empty): (acc, key) =>
      acc.updatedWith(key)(_ => map.get(key))

  def valuesForKeys(keys: Set[String]): KvResponse.Values =
    val forKeys: Map[String, (String, Long)] = getAll(keys)
    KvResponse.Values(RevisionId(forKeys.fmap(_._2)), forKeys.fmap(_._1))

object StateMachine:
  def apply(state: KvState, cmd: KvCommand): (KvState, KvResponse) =
    cmd match
      case KvCommand.Get(keys) =>
        (state, state.valuesForKeys(keys))

      case KvCommand.Update(sets) =>
        (state.setAll(sets), KvResponse.Success)

      case KvCommand.TransactionUpdate(gets, sets) if state.revisionMatches(gets) =>
        (state.setAll(sets), KvResponse.Success)

      case KvCommand.TransactionUpdate(r, _) =>
        (state, state.valuesForKeys(r.revisions.keySet))
