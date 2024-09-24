package com.stoufexis.raft.kvstore.statemachine

import cats.implicits.given

import com.stoufexis.raft.typeclass.Empty

case class KvState(map: Map[String, (String, Long)], revision: Long) derives Empty:
  def setAll(sets: Map[String, Option[String]]): KvState =
    val newr: Long = revision + 1

    val vals: Map[String, (String, Long)] =
      sets.foldLeft(map): 
        case (acc, (k, v)) =>
          acc.updatedWith(k)(_ => v.tupleRight(newr))
    
    KvState(vals, newr)

  def revisionMatches(rid: RevisionId): Boolean =
    rid.revisions.forall((k, r) => map.get(k).exists(_._2 == r))

  def getAll(keys: Set[String]): Map[String, (String, Long)] =
    keys.foldLeft(Map.empty): (acc, key) =>
      acc.updatedWith(key)(_ => map.get(key))

  def valuesForKeys(keys: Set[String]): KvResponse.Values =
    val forKeys: Map[String, (String, Long)] = getAll(keys)
    KvResponse.Values(RevisionId(forKeys.fmap(_._2)), forKeys.fmap(_._1))
