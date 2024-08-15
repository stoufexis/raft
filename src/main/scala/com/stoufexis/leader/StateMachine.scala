package com.stoufexis.leader

import cats.effect.kernel.*
import cats.effect.std.*
import cats.implicits.given
import cats.instances.queue
import com.stoufexis.leader.StateMachine.*
import com.stoufexis.leader.model.*
import com.stoufexis.leader.util.*
import fs2.Pull
import fs2.concurrent.Signal
import fs2.concurrent.SignallingRef
import org.typelevel.log4cats.Logger

trait StateMachine[F[_]]:
  def apply(f: (Term, NodeState, SignalMajorityReached[F]) => F[(Term, NodeState)]): Resource[F, Update[F]]

object StateMachine:
  trait Update[F[_]]:
    // Should cancel any supervised task BEFORE updating the state
    def update[B](f: (Term, NodeState) => ((Term, NodeState), F[B])): F[B]

    def waitUntilMajorityReached: F[Boolean]

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
        // We supervise onSuccess on a separate fiber.
        // If it was part of the main supervised fiber and onSuccess its self
        // called swap, it would attempt to cancel its self, which will probably hang forever
        // Thus, onSuccess is never cancelled explicitly, it simply gets called whenever the
        // task fiber terminates.
        // This makes it so users do not have a handle on the onSuccess fiber and cannot guarantee
        // when it gets called, which may give rise to race conditions that should be guarded against.
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
              _        <- existing.fold(F.unit)(_.cancel)
              _        <- superviseAndSet(task, onSuccess)
              _        <- semaphore.release
            yield ()

  trait SignalMajorityReached[F[_]]:
    def signal: F[Unit]

  def apply[F[_]](using F: Concurrent[F], log: Logger[F]): StateMachine[F] = new:
    def apply(
      stateMachine: (Term, NodeState, SignalMajorityReached[F]) => F[(Term, NodeState)]
    ): Resource[F, Update[F]] =
      for
        supervisor: SingleSpotSupervisor[F] <-
          SingleSpotSupervisor[F]

        semaphore: Semaphore[F] <-
          Resource.eval(Semaphore[F](1))

        state: Ref[F, (Term, NodeState)] <-
          Resource.eval(Ref.of(Term.init, NodeState.Follower))

        majorityReachedRef: SignallingRef[F, Boolean] <-
          Resource.eval(SignallingRef.of[F, Boolean](false))
      yield new:
        def waitUntilMajorityReached: F[Boolean] =
          majorityReachedRef
            .discrete
            .take(2)
            .forall(identity)
            .compileFirstOrError

        val signalmr: SignalMajorityReached[F] = new:
          def signal: F[Unit] =
            majorityReachedRef.set(true)

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
                  val nextState: F[(Term, NodeState)] =
                    stateMachine(t2, n2, signalmr)

                  // When swap executes in this fiber we cannot make sure that
                  // the onSuccess of the previous task is not just in the process
                  // of changing the state. We guard against this race condition
                  // by only allowing this nextTransition to modify the state if it is unchanged.
                  // Since reading and updating the state is protected by a semaphore,
                  // if this effect sees the state as unchanged, it is safe to update it.
                  // Then the onSuccess of the previous task will fail since it executes the same condition.
                  val nextTransition: ((Term, NodeState)) => F[Unit] =
                    (term, nodeState) => setIf((t, n) => t == t2 && n == n2, term, nodeState)

                  for
                    _ <- poll(majorityReachedRef.set(false))
                    _ <- poll(supervisor.swap(nextState, nextTransition))
                    _ <- state.set(t2, n2)
                  yield ()

              _ <- semaphore.release
              b <- poll(fb)
            yield b
