package com.stoufexis.leader.model

enum NodeState derives CanEqual:
  case Follower, Candidate, Leader, VotedFollower

  def print(term: Term): String =
    s"${this.toString}(term = ${term.toInt})"
    

object NodeState:
  def init: NodeState = NodeState.Follower