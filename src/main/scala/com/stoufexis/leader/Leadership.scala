package com.stoufexis.leader

import com.stoufexis.leader.model.*
import com.stoufexis.leader.rpc.RPC

trait Leadership[F[_], A]:
  def currentTerm: F[Term]

  def currentState: F[NodeState]

  def discoverLeaderOf(a: A): F[Discovery]

object Leadership:
  def apply[F[_], A](using rpc: RPC[F, A]): Leadership[F, A] = ???