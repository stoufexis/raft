package com.stoufexis.leader.model

import scala.math.Ordered.orderingToOrdered

case class NodeState(role: Role, term: Term, currentNode: NodeId, otherNodes: Set[NodeId]):
  def print: String =
    s"${role.toString}(term = ${term.toInt})"

  def isExternalMajority(externals: Set[NodeId]): Boolean =
    (otherNodes intersect externals).size >= otherNodes.size

  def transition(newRole: Role, termf: Term => Term): NodeState =
    copy(role = newRole, term = termf(term))

  def isNew(otherTerm: Term): Boolean =
    otherTerm > term

  def isNotNew(otherTerm: Term): Boolean =
    !isNew(otherTerm)

  def transition(newRole: Role): NodeState =
    copy(role = newRole)

object NodeState:
  def init(currentNode: NodeId, otherNodes: Set[NodeId]): NodeState =
    NodeState(Role.Follower, Term.init, currentNode, otherNodes)
