package com.stoufexis.raft.statemachine

import cats.implicits.given

import com.stoufexis.raft.model.*
import com.stoufexis.raft.typeclass.IntLike.given

object Pure:
  // TODO Write test for this
  def commitIdxFromMatch(externalNodes: Set[NodeId], matchIdxs: Map[NodeId, Index]): Option[Index] =
    matchIdxs
      .filter((n, _) => externalNodes(n))
      .toVector
      .map(_._2)
      .sorted(using Ordering[Index].reverse)
      .get(externalNodes.size / 2 - 1)
