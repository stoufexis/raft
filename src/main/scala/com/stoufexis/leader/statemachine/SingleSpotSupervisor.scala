package com.stoufexis.leader.statemachine

import cats.effect.kernel.*
import cats.effect.std.*
import cats.implicits.given

class SingleSpotSupervisor[F[_]](
  supervisor:  Supervisor[F],
  semaphore:   Semaphore[F],
  currentTask: Ref[F, Option[Fiber[F, Throwable, ?]]]
)(using F: Concurrent[F]):
  // We supervise onSuccess on a separate fiber.
  // If it was part of the main supervised fiber and onSuccess its self
  // called swap, it would attempt to cancel its self, which will probably hang forever
  // Thus, onSuccess is never cancelled explicitly, it simply gets called whenever the
  // task fiber terminates.
  // This makes it so users do not have a handle on the onSuccess fiber and cannot guarantee
  // when it gets called, which may give rise to race conditions that should be guarded against.
  private def superviseAndSet[A](task: F[A], onSuccess: A => F[Unit]): F[Unit] =
    for
      fib <- supervisor.supervise(task)
      post = fib.join.flatMap(_.fold(F.unit, _ => F.unit, _ >>= onSuccess))
      _ <- supervisor.supervise(post)
      _ <- currentTask.set(Some(fib))
    yield ()

  def swap[A](task: F[A], onSuccess: A => F[Unit]): F[Unit] =
    // TODO: Does this uncancellable affect tasks handled by the supervisor?
    F.uncancelable: poll =>
      for
        _        <- poll(semaphore.acquire)
        existing <- poll(currentTask.get)
        _        <- existing.fold(F.unit)(_.cancel)
        _        <- superviseAndSet(task, onSuccess)
        _        <- semaphore.release
      yield ()

object SingleSpotSupervisor:
  def apply[F[_]: Concurrent]: Resource[F, SingleSpotSupervisor[F]] =
    for
      supervisor  <- Supervisor[F](await = false)
      semaphore   <- Resource.eval(Semaphore[F](1))
      currentTask <- Resource.eval(Ref.of[F, Option[Fiber[F, Throwable, ?]]](None))
    yield new SingleSpotSupervisor(supervisor, semaphore, currentTask)