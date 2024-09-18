package com.stoufexis.raft.statemachine

import com.stoufexis.raft.model.*
import com.stoufexis.raft.typeclass.IntLike.*

case class NodeInfo(
  role:        Role,
  term:        Term,
  knownLeader: Option[NodeId]
):
  def print: String =
    s"${role.toString}(term = ${term.toLong})"

  def toFollower(newTerm: Term, leaderId: NodeId): NodeInfo =
    copy(role = Role.Follower(Some(leaderId)), term = newTerm, knownLeader = Some(leaderId))

  def toFollower(leaderId: NodeId): NodeInfo =
    copy(role = Role.Follower(Some(leaderId)), knownLeader = Some(leaderId))

  def toFollowerUnknownLeader(newTerm: Term): NodeInfo =
    copy(role = Role.Follower(None), term = newTerm, knownLeader = None)

  def toFollowerUnknownLeader: NodeInfo =
    copy(role = Role.Follower(None), knownLeader = None)

  def toVotedFollower(votedFor: NodeId): NodeInfo =
    copy(role = Role.Follower(Some(votedFor)), knownLeader = None)

  def toCandidateNextTerm: NodeInfo =
    copy(role = Role.Candidate, term = term + 1)

  def toLeader(using cluster: Cluster[?, ?]): NodeInfo =
    copy(role = Role.Leader, knownLeader = Some(cluster.currentNode))

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

  def isCurrentLeader(otherTerm: Term, node: NodeId): Boolean =
    isCurrent(otherTerm) && isLeader(node)

  def isVotee(node: NodeId): Boolean =
    role match
      case Role.Follower(vf) => vf.exists(_ == node)
      case _                 => false

  def isCurrentVotee(otherTerm: Term, node: NodeId): Boolean =
    isCurrent(otherTerm) && isVotee(node)

  def hasVoted: Boolean =
    role match
      case Role.Follower(Some(_)) => true
      case _                      => false

  def votedFor: Option[NodeId] =
    role match
      case Role.Follower(vf) => vf
      case _                 => None
