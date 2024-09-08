package com.stoufexis.raft.statemachine

import cats.*
import cats.effect.kernel.*
import cats.implicits.given
import fs2.*
import fs2.concurrent.Channel
import org.typelevel.log4cats.Logger

import com.stoufexis.raft.model.*
import com.stoufexis.raft.persist.*
import com.stoufexis.raft.rpc.*

import scala.concurrent.duration.FiniteDuration

object StateMachine:
  def runLoop[F[_], A, S](
    currentNode:       NodeId,
    otherNodes:        Set[NodeId],
    heartbeatEvery:    FiniteDuration,
    appenderBatchSize: Int,
    automaton:         (S, A) => S
  )(using
    F:              Async[F],
    monoid:         Monoid[S],
    rpc:            RPC[F, A, S],
    log:            Log[F, A],
    timeout:        Timeout[F],
    persistedState: PersistedState[F]
  ): F[Nothing] =
    def persistIfChanged(oldState: NodeInfo, newState: NodeInfo): F[Unit] =
      if oldState.term != newState.term || oldState.votedFor != newState.votedFor then
        persistedState.persist(newState.term, newState.votedFor)
      else
        F.unit

    def go(st: NodeInfo, chan: Channel[F, NodeInfo]): Stream[F, Nothing] =
      val behaviors: Resource[F, Behaviors[F]] =
        for
          given Logger[F] <-
            Resource
              .eval(NamedLogger[F].fromState(st))
              .evalTap(_.info("Transitioned"))

          behaviors <- st.role match
            case Role.Follower(_) => Resource.pure(Follower(st))
            case Role.Candidate   => Resource.pure(Candidate(st))
            case Role.Leader      => Leader(st, heartbeatEvery, appenderBatchSize, automaton)
        yield behaviors

      // Works like parJoinUnbounded, but reuses the same channel and only ever outputs 1 element
      val joined: Stream[F, NodeInfo] =
        for
          fib: Fiber[F, Throwable, Unit] <-
            Stream.supervise(behaviors.use(_.parPublish(chan)))

          newState: NodeInfo <-
            chan.stream.head.evalTap(fib.cancel >> persistIfChanged(st, _))
        yield newState

      joined >>= (go(_, chan))
    end go

    for
      (lastTerm, lastVoted) <-
        persistedState.readLatest

      initState: NodeInfo =
        NodeInfo(
          role        = Role.Follower(lastVoted),
          term        = lastTerm,
          knownLeader = None,
          currentNode = currentNode,
          otherNodes  = otherNodes
        )

      chan: Channel[F, NodeInfo] <-
        Channel.synchronous

      n: Nothing <-
        go(initState, chan).compile[F, F, Nothing].lastOrError
    yield n
