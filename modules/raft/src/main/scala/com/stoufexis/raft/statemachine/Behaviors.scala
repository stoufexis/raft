package com.stoufexis.raft.statemachine

import cats.effect.implicits.given
import cats.effect.kernel.*
import cats.implicits.given
import fs2.*

case class Behaviors[F[_]](streams: List[Stream[F, NodeInfo]]):
  def raceFirst(using F: Concurrent[F]): F[NodeInfo] =
    for
      ni     <- Deferred[F, NodeInfo]
      joined <- F.pure(streams.parTraverse_(_.head.evalTap(ni.complete).compile.drain))
      out    <- F.race(joined >> F.never[Nothing], ni.get)
    yield out.merge

  infix def ++(others: Seq[Stream[F, NodeInfo]]): Behaviors[F] =
    Behaviors(streams ++ others)

object Behaviors:
  def apply[F[_]](head: Stream[F, NodeInfo], tail: Stream[F, NodeInfo]*): Behaviors[F] =
    Behaviors(head :: tail.toList)
