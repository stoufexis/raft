package com.stoufexis.raft.statemachine

import scala.concurrent.duration.FiniteDuration

// TODO Rename this trait
trait Timeout[F[_]]:
  def nextElectionTimeout: F[FiniteDuration]