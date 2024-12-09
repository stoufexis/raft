package com.stoufexis.raft.statemachine

import cats.effect.kernel.*
import cats.implicits.given
import fs2.*
import fs2.concurrent.Topic

trait BufferedPublisher[F[_], A]:
  def publish(a: A): F[Boolean]

  def publishOrThrow(a: A): F[Unit]

trait BufferedTopic[F[_], A] extends BufferedPublisher[F, A]:
  // TODO: remove from BufferedTopic its inheritted from BufferedPublisher
  def publish(a: A): F[Boolean]
  // TODO: remove from BufferedTopic its inheritted from BufferedPublisher
  def publishOrThrow(a: A): F[Unit]

  def newSubscriber: Resource[F, Stream[F, A]]

object BufferedTopic:
  def apply[F[_], A](using F: Concurrent[F]): Resource[F, BufferedTopic[F, A]] =
    for
      topic    <- Resource.eval(Topic[F, A])
      shutdown <- Resource.eval(Deferred[F, Unit])
      _        <- Resource.onFinalize(shutdown.complete(()).void >> topic.close.void)
    yield new:
      def publish(a: A): F[Boolean] =
        F.race(shutdown.get, topic.publish1(a).map(_.isRight))
          .map(_.fold(_ => false, identity))

      def publishOrThrow(a: A): F[Unit] =
        publish(a).ifM(
          F.unit,
          F.raiseError(IllegalStateException("Topic used after closing"))
        )

      def newSubscriber: Resource[F, Stream[F, A]] =
        topic.subscribeAwaitUnbounded.map(_.interruptWhen(shutdown.get.attempt))
