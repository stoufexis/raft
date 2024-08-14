package com.stoufexis.leader

import cats.effect.kernel.*
import cats.effect.std.*
import cats.implicits.given
import com.stoufexis.leader.StateMachine.*
import com.stoufexis.leader.model.*
import fs2.*
import org.typelevel.log4cats.Logger

trait StateMachine[F[_]]:
  def apply(f: (Term, NodeState, SignalMajorityReached[F]) => F[(Term, NodeState)]): Update[F]

  def waitUntilMajorityReached: F[Unit]

object StateMachine:
  trait Update[F[_]]:
    // Should cancel any supervised task BEFORE updating the state
    def update[B](f: (Term, NodeState) => (Option[(Term, NodeState)], F[B])): F[B]

  trait SingleSpotSupervisor[F[_]]:
    def swap(task: F[Unit]): F[Unit]

  object SingleSpotSupervisor:
    def apply[F[_]](using F: Concurrent[F]): Resource[F, SingleSpotSupervisor[F]] =
      for
        supervisor: Supervisor[F] <-
          Supervisor[F](await = false)

        semaphore: Semaphore[F] <-
          Resource.eval(Semaphore[F](1))

        currentTask: Ref[F, Option[Fiber[F, Throwable, Unit]]] <-
          Resource.eval(Ref.of(None))
      yield new:
        def swap(task: F[Unit]): F[Unit] =
          F.uncancelable: poll =>
            for
              _ <- poll(semaphore.acquire)

              t <- poll(currentTask.get)

              _ <- t match
                case None =>
                  for
                    fib <- supervisor.supervise(task)
                    _   <- currentTask.set(Some(fib))
                  yield ()

                case Some(existing) =>
                  for
                    _   <- existing.cancel
                    fib <- supervisor.supervise(task)
                    _   <- currentTask.set(Some(fib))
                  yield ()

              _ <- semaphore.release
            yield ()

  trait SignalMajorityReached[F[_]]:
    def signal: F[Unit]

  def apply[F[_]](using F: Concurrent[F], log: Logger[F]): Resource[F, StateMachine[F]] =
    for
      supervisor <- SingleSpotSupervisor[F]
      semaphore  <- Resource.eval(Semaphore[F](1))
      state      <- Resource.eval(Ref.of(Term.init, NodeState.Follower))
    yield new:
      def waitUntilMajorityReached: F[Unit] =
        ???

      def apply(
        sm: (Term, NodeState, SignalMajorityReached[F]) => F[(Term, NodeState)]
      ): Update[F] =
        new:
          val signalmr: SignalMajorityReached[F] =
            ???

          def setIf(
            condition:    (Term, NodeState) => Boolean,
            setTerm:      Term,
            setNodeState: NodeState
          ): F[Unit] =
            update: (term, nodeState) =>
              if condition(term, nodeState) then
                Some(setTerm, setNodeState) -> F.unit
              else
                None -> F.unit

          def update[B](f: (Term, NodeState) => (Option[(Term, NodeState)], F[B])): F[B] =
            F.uncancelable: poll =>
              for
                _        <- poll(semaphore.acquire)
                (t1, n1) <- poll(state.get)

                (maybeNewState, fb) = f(t1, n1)

                _ <- maybeNewState match
                  case None => F.unit

                  case Some((t2, n2)) =>
                    val newTask: F[Unit] =
                      for
                        (taskTerm, taskNodeState) <-
                          sm(t2, n2, signalmr)

                        _ <-
                          setIf((t, n) => t == t2 && n == n2, taskTerm, taskNodeState)
                      yield ()

                    for
                      _ <- poll(supervisor.swap(newTask))
                      _ <- state.set(t2, n2)
                    yield ()

                _ <- semaphore.release
                b <- poll(fb)
              yield b
