package com.stoufexis.leader.util

import scala.concurrent.duration.FiniteDuration

// TODO Rename this trait
trait Timeout[F[_]]:
  /** How much to wait for heartbeat responses before the leader assumes it has become stale
    */
  val assumeStaleAfter: FiniteDuration

  val heartbeatRate: FiniteDuration

  def nextElectionTimeout: F[FiniteDuration]
