package com.stoufexis.leader.statemachine

import cats.effect.kernel.*
import cats.effect.std.*
import cats.implicits.given
import com.stoufexis.leader.model.*
import com.stoufexis.leader.statemachine.ConfirmLeader.AckNack
import com.stoufexis.leader.statemachine.StateMachine.Update
import org.typelevel.log4cats.Logger

trait StateMachine[F[_]]:
  def apply(f: (Term, NodeState, AckNack[F]) => F[(Term, NodeState)]): Resource[F, Update[F]]

object StateMachine:
  trait Update[F[_]]:
    // Should cancel any supervised task BEFORE updating the state
    def update[B](f: (Term, NodeState) => ((Term, NodeState), F[B])): F[B]

    def waitUntilMajorityReached: F[Boolean]

  def apply[F[_]](using F: Concurrent[F], log: Logger[F]): StateMachine[F] = new:
    def apply(
      stateMachine: (Term, NodeState, AckNack[F]) => F[(Term, NodeState)]
    ): Resource[F, Update[F]] =
      for
        supervisor: SingleSpotSupervisor[F] <-
          SingleSpotSupervisor[F]

        state: AtomicCell[F, (Term, NodeState)] <-
          Resource.eval(AtomicCell[F].of(Term.init, NodeState.Follower))

        confirmLeader: ConfirmLeader[F] <-
          Resource.eval(ConfirmLeader[F])
      yield new:
        def waitUntilMajorityReached: F[Boolean] =
          confirmLeader.await

        def setIf(
          cond:         (Term, NodeState) => Boolean,
          setTerm:      Term,
          setNodeState: NodeState
        ): F[Unit] = update:
          case (term, nodeState) if cond(term, nodeState) => (setTerm, setNodeState) -> F.unit
          case (term, nodeState)                          => (term, nodeState) -> F.unit

        def update[B](f: (Term, NodeState) => ((Term, NodeState), F[B])): F[B] =
          for
            postUpdate: F[B] <-
              state.evalModify: (t1, n1) =>
                val st @ ((t2, n2), fb) = f(t1, n1)

                if t1 == t2 && n1 == n2 then
                  F.pure(st)
                else
                  val nextState: F[(Term, NodeState)] =
                    confirmLeader.scopedAcks.use: s =>
                      stateMachine(t2, n2, s)

                  /** When swap executes in this fiber we cannot make sure that the onSuccess of the
                    * previous task is not just in the process of changing the state. We guard
                    * against this race condition by only allowing this nextTransition to modify the
                    * state if it is unchanged. Since reading and updating the state is protected by
                    * a mutex, if this effect sees the state as unchanged, it is safe to update it.
                    * Then the onSuccess of the previous task will fail since it executes the same
                    * condition.
                    */
                  val nextTransition: ((Term, NodeState)) => F[Unit] =
                    (term, nodeState) => setIf((t, n) => t == t2 && n == n2, term, nodeState)

                  supervisor.swap(nextState, nextTransition) as st

            b: B <- postUpdate
          yield b
