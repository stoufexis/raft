package com.stoufexis.raft.model

import com.stoufexis.raft.typeclass.IntLike.*

case class NodeInfo[S](
  role:        Role,
  term:        Term,
  knownLeader: Option[NodeId],
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

  def toFollower(newTerm: Term, leaderId: NodeId): NodeInfo[S] =
    copy(role = Role.Follower(None), term = newTerm, knownLeader = Some(leaderId))

  def toFollower(leaderId: NodeId): NodeInfo[S] =
    copy(role = Role.Follower(None), knownLeader = Some(leaderId))

  def toFollowerUnknownLeader(newTerm: Term): NodeInfo[S] =
    copy(role = Role.Follower(None), term = newTerm, knownLeader = None)

  def toFollowerUnknownLeader: NodeInfo[S] =
    copy(role = Role.Follower(None), knownLeader = None)

  def toVotedFollower(candidateId: NodeId, newTerm: Term): NodeInfo[S] =
    copy(role = Role.Follower(Some(candidateId)), term = newTerm)

  def toCandidateNextTerm: NodeInfo[S] =
    copy(role = Role.Candidate, term = term + 1)

  def toLeader: NodeInfo[S] =
    copy(role = Role.Leader, knownLeader = Some(currentNode))

  def isNew(otherTerm: Term): Boolean =
    otherTerm > term

  def isNotNew(otherTerm: Term): Boolean =
    !isNew(otherTerm)

  def isExpired(otherTerm: Term): Boolean =
    otherTerm < term

  def isCurrent(otherTerm: Term): Boolean =
    otherTerm == term

  def isLeader(node: NodeId): Boolean =
    knownLeader.exists(_ == node)

  def votedFor(candidateId: NodeId): Boolean =
    role match
      case Role.Follower(vfor) => vfor.exists(_ == candidateId)
      case Role.Candidate      => candidateId == currentNode
      case Role.Leader         => candidateId == currentNode
