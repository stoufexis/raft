package com.stoufexis.leader.model

enum NodeState:
  case Follower, Candidate, Leader
  case VotedFollower(votedFor: NodeId)

object NodeState:
  def init: NodeState = NodeState.Follower
