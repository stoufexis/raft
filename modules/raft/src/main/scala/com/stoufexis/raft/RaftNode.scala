package com.stoufexis.raft

import cats.effect.kernel.*
import cats.effect.std.Supervisor
import cats.kernel.Monoid
import fs2.Chunk

import com.stoufexis.raft.model.NodeId
import com.stoufexis.raft.persist.*
import com.stoufexis.raft.rpc.*
import com.stoufexis.raft.statemachine.*

import scala.concurrent.duration.*

trait RaftNode[F[_], A, S]:
  def requestVote(req: RequestVote): F[VoteResponse]

  def appendEntries(req: AppendEntries[A]): F[AppendResponse]

  def clientRequest(entries: Chunk[A]): F[ClientResponse[S]]

object RaftNode:
  case class Builder[F[_], A, S](
    id:                    NodeId,
    automaton:             (S, A) => S,
    log:                   Log[F, A],
    persisted:             PersistedState[F],
    otherNodes:            List[ExternalNode[F, A, S]] = Nil,
    heartbeatEvery:        FiniteDuration              = 100.millis,
    electionTimeoutLow:    FiniteDuration              = 500.millis,
    electionTimeoutHigh:   FiniteDuration              = 1.second,
    appenderBatchSize:     Int                         = 10,
    clientRequestsBuffer:  Int                         = 10,
    incomingVotedBuffer:   Int                         = 1,
    incomingAppendsBuffer: Int                         = 1
  ):
    def withExternals(nodes: ExternalNode[F, A, S]*): Builder[F, A, S] =
      copy(otherNodes = otherNodes ++ nodes)

    def withHeartbeatEvery(heartbeatEvery: FiniteDuration): Builder[F, A, S] =
      copy(heartbeatEvery = heartbeatEvery)

    def withElectionTimeout(from: FiniteDuration, until: FiniteDuration): Builder[F, A, S] =
      copy(electionTimeoutLow = from, electionTimeoutHigh = until)

    def withAppenderBatchSize(size: Int): Builder[F, A, S] =
      copy(appenderBatchSize = size)

    def build(using Async[F], Monoid[S]): Resource[F, RaftNode[F, A, S]] =
      for
        supervisor: Supervisor[F] <-
          Supervisor[F](await = false)

        inputs: Inputs[F, A, S] <-
          Resource.eval(Inputs(clientRequestsBuffer, incomingVotedBuffer, incomingAppendsBuffer))

        timeout: Timeout[F] <-
          Resource.eval(Timeout.fromRange(electionTimeoutLow, electionTimeoutHigh))

        cfg: Config[F, A, S] =
          Config(
            automaton         = automaton,
            log               = log,
            persisted         = persisted,
            cluster           = Cluster(id, otherNodes),
            heartbeatEvery    = heartbeatEvery,
            timeout           = timeout,
            inputs            = inputs,
            appenderBatchSize = appenderBatchSize
          )

        _ <-
          Resource.eval(supervisor.supervise(StateMachine.runLoop(cfg)))
      yield new:
        def requestVote(req: RequestVote): F[VoteResponse] =
          inputs.requestVote(req)

        def appendEntries(req: AppendEntries[A]): F[AppendResponse] =
          inputs.appendEntries(req)

        def clientRequest(entries: Chunk[A]): F[ClientResponse[S]] =
          inputs.clientRequest(entries)
