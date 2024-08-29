package com.stoufexis.leader.statemachine

import cats.effect.kernel.*
import cats.implicits.given
import fs2.*
import fs2.concurrent.Topic
import fs2.concurrent.Topic.Closed

class CloseableTopic[F[_], A](topic: Topic[F, A], shutdown: Deferred[F, Unit])(using
  F: Concurrent[F]
):
  def subscribe: Stream[F, A] =
    Stream.eval(shutdown.tryGet).flatMap:
      case Some(()) =>
        Stream.empty

      case None =>
        topic
          .subscribeUnbounded
          .interruptWhen(shutdown.get.attempt)

  def publish(a: A): F[Boolean] =
    shutdown.tryGet.flatMap:
      case Some(()) =>
        F.pure(false)

      case None =>
        topic.publish1(a).flatMap:
          case Left(_)   => F.pure(false)
          case Right(()) => F.pure(true)

  def close: F[Unit] =
    shutdown.complete(()).void >> topic.close.void

object CloseableTopic:
  def apply[F[_]: Concurrent, A]: F[CloseableTopic[F, A]] =
    (Topic[F, A], Deferred[F, Unit])
      .mapN(new CloseableTopic(_, _))
