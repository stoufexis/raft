package com.stoufexis.leader.model

import com.stoufexis.leader.typeclass.IntLike.*

case class NodeInfo[S](
  role:           Role,
  term:           Term,
  currentNode:    NodeId,
  otherNodes:     Set[NodeId],
):
  def print: String =
    s"${role.toString}(term = ${term.toInt})"

  def allNodes: Set[NodeId] =
    otherNodes + currentNode

  def transition(newRole: Role, termf: Term => Term): NodeInfo[S] =
    copy(role = newRole, term = termf(term))

  def transition(newRole: Role, newTerm: Term): NodeInfo[S] =
    copy(role = newRole, term = newTerm)

  def transition(newRole: Role): NodeInfo[S] =
    copy(role = newRole)

  def toFollower(newTerm: Term): NodeInfo[S] =
    transition(Role.Follower, newTerm)

  def isNew(otherTerm: Term): Boolean =
    otherTerm > term

  def isNotNew(otherTerm: Term): Boolean =
    !isNew(otherTerm)

  def isExpired(otherTerm: Term): Boolean =
    otherTerm < term

  def isCurrent(otherTerm: Term): Boolean =
    otherTerm == term
