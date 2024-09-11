package com.stoufexis.raft.statemachine

import com.stoufexis.raft.ExternalNode
import com.stoufexis.raft.model.*

case class Cluster[F[_], A, S](
  currentNode: NodeId,
  otherNodes:  List[ExternalNode[F, A, S]]
):
  lazy val allNodes: Set[NodeId] =
    otherNodes.map(_.id).toSet + currentNode

  lazy val otherNodeIds: Set[NodeId] =
    otherNodes.map(_.id).toSet

  lazy val majorityCnt: Int =
    allNodes.size / 2 + 1

  def isMajority(nodes: Set[NodeId]): Boolean =
    (nodes intersect allNodes).size >= majorityCnt
