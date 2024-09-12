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

trait RaftNode[F[_], A, S]:
  def requestVote(req: RequestVote): F[VoteResponse]

  def appendEntries(req: AppendEntries[A]): F[AppendResponse]

  def clientWrite(client: ClientId, serial: SerialNr, entry: A): F[ClientResponse[S]]

  def clientWriteNonLinearizable(entry: A): F[ClientResponse[S]]

  def clientRead: F[ClientResponse[S]]

object RaftNode:
  def builder[F[_], A, S](
    id:        NodeId,
    automaton: (S, A) => S,
    log:       Log[F, A],
    persisted: PersistedState[F]
  ): Builder[F, A, S] =
    Builder(id, automaton, log, persisted)

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
      Supervisor[F](await = false).evalMap: supervisor =>
        for
          inputs: Inputs[F, A, S] <-
            Inputs(clientRequestsBuffer, incomingVotedBuffer, incomingAppendsBuffer)

          timeout: ElectionTimeout[F] <-
            ElectionTimeout.fromRange[F](electionTimeoutLow, electionTimeoutHigh)

          given Config[F, A, S] =
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

          def appendEntries(req: AppendEntries[A]): F[AppendResponse] =
            inputs.appendEntries(req)

          def clientWrite(client: ClientId, serial: SerialNr, entry: A): F[ClientResponse[S]] =
            inputs.clientRequest(Some(Command(Some(CommandId(client, serial)), entry)))

          def clientWriteNonLinearizable(entry: A): F[ClientResponse[S]] =
            inputs.clientRequest(Some(Command(None, entry)))

          def clientRead: F[ClientResponse[S]] =
            inputs.clientRequest(None)