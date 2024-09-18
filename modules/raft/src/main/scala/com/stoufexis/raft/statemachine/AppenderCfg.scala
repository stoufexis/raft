package com.stoufexis.raft.statemachine

import scala.concurrent.duration.FiniteDuration

case class AppenderCfg(appenderBatchSize: Int, heartbeatEvery: FiniteDuration)
