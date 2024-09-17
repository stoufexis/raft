package com.stoufexis.raft.kvstore.statemachine

import cats.implicits.given

import com.stoufexis.raft.typeclass.Empty

case class KvState(map: Map[String, (String, Long)]) derives Empty:
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
