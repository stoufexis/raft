package com.stoufexis.leader

import cats.effect.kernel.*
import cats.effect.std.*
import cats.implicits.given
import com.stoufexis.leader.StateMachine.*
import com.stoufexis.leader.model.*
import org.typelevel.log4cats.Logger

trait StateMachine[F[_]]:
  def apply(f: (Term, NodeState, SignalMajorityReached[F]) => F[(Term, NodeState)]): Update[F]

  def waitUntilMajorityReached: F[Unit]

object StateMachine:
  trait Update[F[_]]:
    // Should cancel any supervised task BEFORE updating the state
    def update[B](f: (Term, NodeState) => ((Term, NodeState), F[B])): F[B]

  trait SingleSpotSupervisor[F[_]]:
    def swap[A](task: F[A], onSuccess: A => F[Unit]): F[Unit]

  object SingleSpotSupervisor:
    def apply[F[_]](using F: Concurrent[F]): Resource[F, SingleSpotSupervisor[F]] =
      for
        supervisor: Supervisor[F] <-
          Supervisor[F](await = false)

        semaphore: Semaphore[F] <-
          Resource.eval(Semaphore[F](1))

        currentTask: Ref[F, Option[Fiber[F, Throwable, ?]]] <-
          Resource.eval(Ref.of(None))
      yield new:
        def superviseAndSet[A](task: F[A], onSuccess: A => F[Unit]): F[Unit] =
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
              cancel   <- existing.fold(F.unit)(_.cancel)
              _        <- superviseAndSet(task, onSuccess)
              _        <- semaphore.release
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
                (setTerm, setNodeState) -> F.unit
              else
                (term, nodeState) -> F.unit

          def update[B](f: (Term, NodeState) => ((Term, NodeState), F[B])): F[B] =
            F.uncancelable: poll =>
              for
                _        <- poll(semaphore.acquire)
                (t1, n1) <- poll(state.get)

                ((t2, n2), fb) = f(t1, n1)

                _ <-
                  if t1 == t2 && n1 == n2 then
                    F.unit
                  else
                    for
                      _ <- poll:
                        supervisor.swap(
                          sm(t2, n2, signalmr),
                          setIf((t, n) => t == t2 && n == n2, _, _)
                        )
                      _ <- state.set(t2, n2)
                    yield ()

                _ <- semaphore.release
                b <- poll(fb)
              yield b
