package com.stoufexis.leader.model

enum Role derives CanEqual:
  case Follower, Candidate, Leader
  case VotedFollower(candidate: NodeId)

object Role:
  def init: Role = Role.Follower
