package com.stoufexis.leader.model

import com.stoufexis.leader.typeclass.Increasing.*

case class NodeState(role: Role, term: Term, currentNode: NodeId, otherNodes: Set[NodeId]):
  def print: String =
    s"${role.toString}(term = ${term.toInt})"

  def isExternalMajority(externals: Set[NodeId]): Boolean =
    (otherNodes intersect externals).size >= otherNodes.size

  def transition(newRole: Role, termf: Term => Term): NodeState =
    copy(role = newRole, term = termf(term))

  def transition(newRole: Role, newTerm: Term): NodeState =
    copy(role = newRole, term = newTerm)

  def isNew(otherTerm: Term): Boolean =
    otherTerm > term

  def isNotNew(otherTerm: Term): Boolean =
    !isNew(otherTerm)

  def isExpired(otherTerm: Term): Boolean =
    otherTerm < term

  def isCurrent(otherTerm: Term): Boolean =
    otherTerm == term

  def transition(newRole: Role): NodeState =
    copy(role = newRole)