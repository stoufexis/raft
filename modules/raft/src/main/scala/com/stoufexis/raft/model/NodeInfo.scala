package com.stoufexis.raft.model

import com.stoufexis.raft.typeclass.IntLike.*

case class NodeInfo[S](
  role:        Role,
  term:        Term,
  currentNode: NodeId,
  otherNodes:  Set[NodeId]
):
  def print: String =
    s"${role.toString}(term = ${term.toInt})"

  def allNodes: Set[NodeId] =
    otherNodes + currentNode

  def majorityCnt: Int =
    allNodes.size / 2 + 1

  def isMajority(nodes: Set[NodeId]): Boolean =
    (nodes intersect allNodes).size >= majorityCnt

  def transition(newRole: Role, termf: Term => Term): NodeInfo[S] =
    copy(role = newRole, term = termf(term))

  def transition(newRole: Role, newTerm: Term): NodeInfo[S] =
    copy(role = newRole, term = newTerm)

  def transition(newRole: Role): NodeInfo[S] =
    copy(role = newRole)

  def toFollower(newTerm: Term): NodeInfo[S] =
    transition(Role.Follower(None), newTerm)

  def toFollower: NodeInfo[S] =
    transition(Role.Follower(None))

  def toVotedFollower(candidateId: NodeId, newTerm: Term): NodeInfo[S] =
    transition(Role.Follower(Some(candidateId)), newTerm)

  def toCandidateNextTerm: NodeInfo[S] =
    transition(Role.Candidate, term + 1)

  def toLeader: NodeInfo[S] =
    transition(Role.Leader)

  def newTerm(term: Term): NodeInfo[S] =
    copy(term = term)

  def isNew(otherTerm: Term): Boolean =
    otherTerm > term

  def isNotNew(otherTerm: Term): Boolean =
    !isNew(otherTerm)

  def isExpired(otherTerm: Term): Boolean =
    otherTerm < term

  def isCurrent(otherTerm: Term): Boolean =
    otherTerm == term

  def votedFor(candidateId: NodeId): Boolean =
    role match
      case Role.Follower(vfor) => vfor.exists(_ == candidateId)
      case Role.Candidate      => candidateId == currentNode
      case Role.Leader         => candidateId == currentNode
