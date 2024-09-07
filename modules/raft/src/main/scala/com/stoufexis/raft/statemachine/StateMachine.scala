package com.stoufexis.raft.statemachine

import cats.*
import cats.effect.kernel.*
import cats.implicits.given
import fs2.*
import fs2.concurrent.Channel
import org.typelevel.log4cats.Logger

import com.stoufexis.raft.model.*
import com.stoufexis.raft.rpc.*
import com.stoufexis.raft.service.NamedLogger

import scala.concurrent.duration.FiniteDuration

object StateMachine:
  def runLoop[F[_], A, S](
    init:           NodeInfo[S],
    heartbeatEvery: FiniteDuration,
    automaton:      (S, A) => S
  )(using
    Async[F],
    Monoid[S],
    RPC[F, A, S],
    Log[F, A],
    Timeout[F]
  ): F[Nothing] =
    def go(st: NodeInfo[S], chan: Channel[F, NodeInfo[S]]): Stream[F, Nothing] =
      val behaviors: Resource[F, Behaviors[F, S]] =
        for
          given Logger[F] <-
            Resource.eval(NamedLogger[F].fromState(st))

          behaviors <- st.role match
            case Role.Follower  => Resource.eval(Follower(st))
            case Role.Candidate => Resource.eval(Candidate(st))
            case Role.Leader    => Leader(st, heartbeatEvery, automaton)
        yield behaviors

      // Works like parJoinUnbounded, but reuses the same channel and only ever outputs 1 element
      val joined: Stream[F, NodeInfo[S]] =
        for
          fib: Fiber[F, Throwable, Unit] <-
            Stream.supervise(behaviors.use(_.parPublish(chan)))

          newState: NodeInfo[S] <-
            chan.stream.head.evalTap(_ => fib.cancel)
        yield newState

      joined >>= (go(_, chan))
    end go

    for
      chan <- Channel.synchronous[F, NodeInfo[S]]
      n    <- go(init, chan).compile[F, F, Nothing].lastOrError
    yield n
