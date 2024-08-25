package com.stoufexis.leader

import scala.concurrent.duration.FiniteDuration

case class Config(
  staleAfter:     FiniteDuration,
  heartbeatEvery: FiniteDuration,
  voteEvery:      FiniteDuration
)
