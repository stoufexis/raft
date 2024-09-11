package com.stoufexis.raft.statemachine

import cats.effect.implicits.given
import cats.effect.kernel.*
import cats.implicits.given
import fs2.*
import fs2.concurrent.Channel

case class Behaviors[F[_]](streams: Seq[Stream[F, NodeInfo]]):
  def parPublish(chan: Channel[F, NodeInfo])(using Concurrent[F]): F[Unit] =
    streams.parTraverse_(_.evalTap(chan.send(_).void).compile.drain)

  infix def ++(others: Seq[Stream[F, NodeInfo]]): Behaviors[F] =
    Behaviors(streams ++ others)

object Behaviors:
  def apply[F[_]](head: Stream[F, NodeInfo], tail: Stream[F, NodeInfo]*): Behaviors[F] =
    Behaviors(head +: tail)
