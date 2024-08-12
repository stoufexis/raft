package com.stoufexis.leader.util

trait MajorityReachedSignal[F[_]]:
  def majorityReached: F[Unit]