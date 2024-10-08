package com.stoufexis.raft.model

enum Role derives CanEqual:
  case Follower(votedFor: Option[NodeId])
  case Candidate
  case Leader

object Role:
  def init: Role = Role.Follower(None)
