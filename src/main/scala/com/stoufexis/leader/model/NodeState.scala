package com.stoufexis.leader.model

enum NodeState:
  case Follower, Candidate, Leader, VotedFollower

object NodeState:
  def init: NodeState = NodeState.Follower
