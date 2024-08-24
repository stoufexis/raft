package com.stoufexis.leader.model

import com.stoufexis.leader.typeclass.Increasing.*

case class NodeInfo[S](
  role:           Role,
  term:           Term,
  currentNode:    NodeId,
  otherNodes:     Set[NodeId],
  automatonState: S,
):
  def print: String =
    s"${role.toString}(term = ${term.toInt})"

  def isExternalMajority(externals: Set[NodeId]): Boolean =
    (otherNodes intersect externals).size >= otherNodes.size

  def transition(newRole: Role, termf: Term => Term): NodeInfo[S] =
    copy(role = newRole, term = termf(term))

  def transition(newRole: Role, newTerm: Term): NodeInfo[S] =
    copy(role = newRole, term = newTerm)

  def isNew(otherTerm: Term): Boolean =
    otherTerm > term

  def isNotNew(otherTerm: Term): Boolean =
    !isNew(otherTerm)

  def isExpired(otherTerm: Term): Boolean =
    otherTerm < term

  def isCurrent(otherTerm: Term): Boolean =
    otherTerm == term

  def transition(newRole: Role): NodeInfo[S] =
    copy(role = newRole)
