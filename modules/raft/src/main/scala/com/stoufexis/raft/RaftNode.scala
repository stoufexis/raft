package com.stoufexis.raft

import cats.effect.kernel.*
import cats.effect.std.Supervisor
import cats.implicits.given
import cats.kernel.Monoid

import com.stoufexis.raft.model.*
import com.stoufexis.raft.persist.*
import com.stoufexis.raft.rpc.*
import com.stoufexis.raft.statemachine.*

import scala.concurrent.duration.*

trait RaftNode[F[_], In, Out, S]:
  def requestVote(req: RequestVote): F[VoteResponse]

  def appendEntries(req: AppendEntries[In]): F[AppendResponse]

  def clientRequest(cmd: Command[In]): F[ClientResponse[Out, S]]

object RaftNode:
  def builder[F[_], In, Out, S](
    id:        NodeId,
    automaton: (S, In) => (S, Out),
    log:       Log[F, In],
    persisted: PersistedState[F]
  ): Builder[F, In, Out, S] =
    Builder(id, automaton, log, persisted)

  case class Builder[F[_], In, Out, S](
    id:                    NodeId,
    automaton:             (S, In) => (S, Out),
    log:                   Log[F, In],
    persisted:             PersistedState[F],
    otherNodes:            List[ExternalNode[F, In]] = Nil,
    heartbeatEvery:        FiniteDuration            = 100.millis,
    electionTimeoutLow:    FiniteDuration            = 500.millis,
    electionTimeoutHigh:   FiniteDuration            = 1.second,
    appenderBatchSize:     Int                       = 10,
    clientRequestsBuffer:  Int                       = 10,
    incomingVotedBuffer:   Int                       = 1,
    incomingAppendsBuffer: Int                       = 1
  ):
    def withExternals(nodes: ExternalNode[F, In]*): Builder[F, In, Out, S] =
      copy(otherNodes = otherNodes ++ nodes)

    def withHeartbeatEvery(heartbeatEvery: FiniteDuration): Builder[F, In, Out, S] =
      copy(heartbeatEvery = heartbeatEvery)

    def withElectionTimeout(from: FiniteDuration, until: FiniteDuration): Builder[F, In, Out, S] =
      copy(electionTimeoutLow = from, electionTimeoutHigh = until)

    def withAppenderBatchSize(size: Int): Builder[F, In, Out, S] =
      copy(appenderBatchSize = size)

    def build(using Async[F], Monoid[S]): Resource[F, RaftNode[F, In, Out, S]] =
      Supervisor[F](await = false).evalMap: supervisor =>
        for
          inputs: Inputs[F, In, Out, S] <-
            Inputs(clientRequestsBuffer, incomingVotedBuffer, incomingAppendsBuffer)

          timeout: ElectionTimeout[F] <-
            ElectionTimeout.fromRange[F](electionTimeoutLow, electionTimeoutHigh)

          given Config[F, In, Out, S] =
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
            supervisor.supervise(StateMachine.runLoop)
        yield new:
          def requestVote(req: RequestVote): F[VoteResponse] =
            inputs.requestVote(req)

          def appendEntries(req: AppendEntries[In]): F[AppendResponse] =
            inputs.appendEntries(req)

          def clientRequest(cmd: Command[In]): F[ClientResponse[Out, S]] =
            inputs.clientRequest(cmd)
