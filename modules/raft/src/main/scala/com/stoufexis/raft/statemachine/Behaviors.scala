package com.stoufexis.raft.statemachine

import cats.effect.kernel.*
import fs2.*

case class Behaviors[F[_]](streams: List[Stream[F, NodeInfo]]):
  def raceFirst(using Concurrent[F]): F[NodeInfo] =
    streams.map(_.head).parJoinUnbounded.head.compile.lastOrError

  infix def ++(others: Seq[Stream[F, NodeInfo]]): Behaviors[F] =
    Behaviors(streams ++ others)

object Behaviors:
  def apply[F[_]](head: Stream[F, NodeInfo], tail: Stream[F, NodeInfo]*): Behaviors[F] =
    Behaviors(head :: tail.toList)
