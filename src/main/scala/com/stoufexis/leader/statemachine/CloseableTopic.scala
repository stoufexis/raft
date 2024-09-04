package com.stoufexis.leader.statemachine

import cats.effect.kernel.*
import cats.implicits.given
import fs2.*
import fs2.concurrent.Topic

trait CloseableTopic[F[_], A]:
  def publish(a: A): F[Boolean]

  def publishOrThrow(a: A): F[Unit]

  def subscribeUnbounded: Stream[F, A]

  def subscribe(bound: Int): Stream[F, A]

object CloseableTopic:
  def apply[F[_], A](using F: Concurrent[F]): Resource[F, CloseableTopic[F, A]] =
    for
      topic    <- Resource.eval(Topic[F, A])
      shutdown <- Resource.eval(Deferred[F, Unit])
      _        <- Resource.onFinalize(shutdown.complete(()).void >> topic.close.void)
    yield new:
      def subscribeUnbounded: Stream[F, A] =
        subscribe(Int.MaxValue) // this is how the fs2 topic also does it

      def subscribe(bound: Int): Stream[F, A] =
        topic.subscribe(bound).interruptWhen(shutdown.get.attempt)

      def publish(a: A): F[Boolean] =
        F.race(shutdown.get, topic.publish1(a).map(_.isRight))
          .map(_.fold(_ => false, identity))

      def publishOrThrow(a: A): F[Unit] =
        publish(a).ifM(
          F.unit,
          F.raiseError(IllegalStateException("Topic used after closing"))
        )
