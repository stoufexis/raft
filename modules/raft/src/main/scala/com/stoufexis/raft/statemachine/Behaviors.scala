package com.stoufexis.raft.statemachine

import cats.effect.implicits.given
import cats.effect.kernel.*
import cats.implicits.given
import fs2.*
import fs2.concurrent.Channel

import com.stoufexis.raft.model.NodeInfo

case class Behaviors[F[_], S](streams: Seq[Stream[F, NodeInfo]]):
  def parPublish(chan: Channel[F, NodeInfo])(using Concurrent[F]): F[Unit] =
    streams.parTraverse_(_.evalTap(chan.send(_).void).compile.drain)

object Behaviors:
  def apply[F[_], S](head: Stream[F, NodeInfo], tail: Stream[F, NodeInfo]*): Behaviors[F, S] =
    Behaviors(head +: tail)
