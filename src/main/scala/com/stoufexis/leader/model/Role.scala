package com.stoufexis.leader.model

enum Role derives CanEqual:
  case Follower, Candidate, Leader, VotedFollower

object Role:
  def init: Role = Role.Follower