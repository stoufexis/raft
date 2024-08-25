package com.stoufexis.leader.statemachine

import scala.concurrent.duration.FiniteDuration

case class Config(
  staleAfter:     FiniteDuration,
  heartbeatEvery: FiniteDuration,
  voteEvery:      FiniteDuration
)
