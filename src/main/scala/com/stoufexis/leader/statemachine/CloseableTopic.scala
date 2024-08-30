package com.stoufexis.leader.statemachine

import cats.effect.kernel.*
import cats.implicits.given
import fs2.*
import fs2.concurrent.Topic
import fs2.concurrent.Topic.Closed

class CloseableTopic[F[_], A](topic: Topic[F, A], shutdown: Deferred[F, Unit])(using
  F: Concurrent[F]
):
  def subscribeUnbounded: Stream[F, A] =
    subscribe(Int.MaxValue) // this is how the fs2 topic also does it

  def subscribe(bound: Int): Stream[F, A] =
    Stream.eval(shutdown.tryGet).flatMap:
      case Some(()) =>
        Stream.empty

      case None =>
        topic
          .subscribe(bound)
          .interruptWhen(shutdown.get.attempt)

  def publish(a: A): F[Boolean] =
    shutdown.tryGet.flatMap:
      case Some(()) =>
        F.pure(false)

      case None =>
        topic.publish1(a).flatMap:
          case Left(_)   => F.pure(false)
          case Right(()) => F.pure(true)

  def publishOrThrow(a: A): F[Unit] =
    val err: IllegalStateException =
      IllegalStateException("Topic used after closing")

    publish(a).ifM(F.unit, F.raiseError(err))

  def close: F[Unit] =
    shutdown.complete(()).void >> topic.close.void

object CloseableTopic:
  def apply[F[_]: Concurrent, A]: Resource[F, CloseableTopic[F, A]] =
    val make: F[CloseableTopic[F, A]] =
      (Topic[F, A], Deferred[F, Unit])
        .mapN(new CloseableTopic(_, _))

    Resource.make(make)(_.close)
