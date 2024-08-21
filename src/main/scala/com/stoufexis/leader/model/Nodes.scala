package com.stoufexis.leader.model

case class Nodes(currentNode: NodeId, otherNodes: Set[NodeId]):
  def isExternalMajority(externals: Set[NodeId]): Boolean =
    (otherNodes intersect externals).size >= otherNodes.size
