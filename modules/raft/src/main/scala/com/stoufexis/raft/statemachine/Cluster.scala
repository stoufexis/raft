package com.stoufexis.raft.statemachine

import com.stoufexis.raft.ExternalNode
import com.stoufexis.raft.model.*

case class Cluster[F[_], In](
  currentNode: NodeId,
  otherNodes:  List[ExternalNode[F, In]]
):
  lazy val allNodes: Set[NodeId] =
    otherNodeIds + currentNode

  lazy val otherNodeIds: Set[NodeId] =
    otherNodes.map(_.id).toSet

  lazy val otherNodesSize: Int =
    otherNodeIds.size

  lazy val majorityCnt: Int =
    allNodes.size / 2 + 1

  def isMajority(nodes: Set[NodeId]): Boolean =
    (nodes intersect allNodes).size >= majorityCnt
