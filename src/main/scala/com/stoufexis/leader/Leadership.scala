package com.stoufexis.leader

import cats.*
import cats.effect.kernel.*
import cats.implicits.given
import fs2.*
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import com.stoufexis.leader.model.*
import com.stoufexis.leader.rpc.*
import com.stoufexis.leader.util.*

import scala.concurrent.duration.FiniteDuration

/** Raft leadership for a single entity
  */
trait Leadership[F[_]]:
  def currentTerm: F[Term]

  def currentState: F[Role]

  def termStream: Stream[F, Term]

  def stateStream: Stream[F, Role]

  def discoverLeader: F[Discovery]

  def waitUntilConfirmedLeader: F[Boolean]

// TODO: general error handling
object Leadership:

  def stateMachine[F[_]: Async](
    rpc:           RPC[F],
    timeout:       Timeout[F],
    initState:     NodeState,
    heartbeatRate: FiniteDuration,
    voteRate:      FiniteDuration,
    staleAfter:    FiniteDuration
  ): F[Nothing] =
    def loop(nodeState: NodeState): F[Nothing] =
      (for
        given Logger[F] <-
          Slf4jLogger.fromName[F](nodeState.print)

        electionTimeout: FiniteDuration <-
          timeout.nextElectionTimeout

        constStreams: List[Stream[F, NodeState]] = List(
          handleHeartbeats(nodeState, electionTimeout, rpc.incomingHeartbeatRequests),
          handleVoteRequests(nodeState, rpc.incomingVoteRequests)
        )

        specificStream: List[Stream[F, NodeState]] = nodeState.role match
          case Role.Leader =>
            val broadcast: Stream[F, (NodeId, HeartbeatResponse)] =
              rpc.broadcastHeartbeat(nodeState, heartbeatRate)

            List(sendHeartbeats(nodeState, broadcast, staleAfter))

          case Role.Candidate =>
            val broadcast: Stream[F, (NodeId, VoteResponse)] =
              rpc.broadcastVote(nodeState, voteRate)

            List(attemptElection(nodeState, broadcast, electionTimeout))

          case Role.Follower | Role.VotedFollower =>
            Nil

        newState: NodeState <-
          raceFirstOrError(constStreams ++ specificStream)

        _ <- Logger[F].info(s"Transitioning to ${newState.print}")
      yield newState) >>= loop

    loop(initState)
  end stateMachine

  def handleHeartbeats[F[_]](
    state:           NodeState,
    electionTimeout: FiniteDuration,
    incoming:        Stream[F, IncomingHeartbeat[F]]
  )(using F: Temporal[F], log: Logger[F]): Stream[F, NodeState] =

    def termExpired(req: HeartbeatRequest, sink: DeferredSink[F, HeartbeatResponse]): F[Unit] =
      for
        _ <- sink.complete_(HeartbeatResponse.TermExpired(state.term))
        _ <- log.warn(s"Detected stale leader ${req.from}")
      yield ()

    def accepted(req: HeartbeatRequest, sink: DeferredSink[F, HeartbeatResponse]): F[Unit] =
      for
        _ <- sink.complete_(HeartbeatResponse.Accepted)
        _ <- log.info(s"Heartbeat accepted from ${req.from}")
      yield ()

    def duplicateLeaders(req: HeartbeatRequest, sink: DeferredSink[F, HeartbeatResponse]): F[Unit] =
      for
        state <- F.pure("Duplicate leaders for term")
        _     <- sink.complete_(HeartbeatResponse.IllegalState(state))
        _     <- F.raiseError(IllegalStateException(state))
      yield ()

    state.role match
      case Role.Leader =>
        incoming.evalMapFilter:
          case IncomingHeartbeat(request, sink) if state.isExpired(request.term) =>
            termExpired(request, sink) as None

          case IncomingHeartbeat(request, sink) if state.isCurrent(request.term) =>
            duplicateLeaders(request, sink) as None

          case IncomingHeartbeat(request, sink) =>
            accepted(request, sink) as Some(state.transition(Role.Follower, _ => request.term))

      case Role.Follower =>
        incoming.resettableTimeout(
          timeout   = electionTimeout,
          onTimeout = F.pure(state.transition(Role.Candidate, _.next))
        ):
          case IncomingHeartbeat(request, sink) if state.isExpired(request.term) =>
            termExpired(request, sink) as ResettableTimeout.Skip()

          case IncomingHeartbeat(request, sink) if state.isCurrent(request.term) =>
            accepted(request, sink) as ResettableTimeout.Reset()

          case IncomingHeartbeat(request, sink) =>
            accepted(request, sink) as
              ResettableTimeout.Output(state.transition(Role.Follower, _ => request.term))

      case Role.Candidate | Role.VotedFollower =>
        incoming.evalMapFilter:
          case IncomingHeartbeat(request, sink) if state.isExpired(request.term) =>
            termExpired(request, sink) as None

          case IncomingHeartbeat(request, sink) =>
            accepted(request, sink) as Some(state.transition(Role.Follower, _ => request.term))

  def handleVoteRequests[F[_]](
    state:    NodeState,
    incoming: Stream[F, IncomingVoteRequest[F]]
  )(using F: Temporal[F], log: Logger[F]): Stream[F, NodeState] =

    def termExpired(req: VoteRequest, sink: DeferredSink[F, VoteResponse]): F[Unit] =
      for
        _ <- sink.complete_(VoteResponse.TermExpired(state.term))
        _ <- log.warn(s"Detected stale candidate ${req.from}")
      yield ()

    def grant(req: VoteRequest, sink: DeferredSink[F, VoteResponse]): F[Unit] =
      for
        _ <- sink.complete_(VoteResponse.Granted)
        _ <- log.info(s"Voted for ${req.from} in term ${req.term}")
      yield ()

    def reject(req: VoteRequest, sink: DeferredSink[F, VoteResponse]): F[Unit] =
      for
        _ <- sink.complete_(VoteResponse.Rejected)
        _ <- log.info(s"Rejected vote for ${req.from}. Its term was ${req.term}")
      yield ()

    def illegalElection(sink: DeferredSink[F, VoteResponse]): F[Unit] =
      for
        msg <- F.pure(s"New election for current term ${state.term}")
        _   <- sink.complete_(VoteResponse.IllegalState(msg))
        _   <- F.raiseError(IllegalStateException(msg))
      yield ()

    state.role match
      case Role.Leader =>
        incoming.evalMapFilter:
          case IncomingVoteRequest(request, sink) if state.isExpired(request.term) =>
            termExpired(request, sink) as None

          case IncomingVoteRequest(request, sink) if state.isCurrent(request.term) =>
            reject(request, sink) as None

          case IncomingVoteRequest(request, sink) =>
            grant(request, sink) as Some(state.transition(Role.VotedFollower, _ => request.term))

      case Role.Follower =>
        incoming.evalMapFilter:
          case IncomingVoteRequest(request, sink) if state.isExpired(request.term) =>
            termExpired(request, sink) as None

          case IncomingVoteRequest(request, sink) if request.term == state.term =>
            illegalElection(sink) as None

          case IncomingVoteRequest(request, sink) =>
            grant(request, sink) as Some(state.transition(Role.VotedFollower, _ => request.term))

      case Role.VotedFollower | Role.Candidate =>
        incoming.evalMapFilter:
          case IncomingVoteRequest(request, sink) if state.isExpired(request.term) =>
            termExpired(request, sink) as None

          case IncomingVoteRequest(request, sink) =>
            reject(request, sink) as None

  /** Repeatedly broadcasts heartbeat requests to all other nodes. Calls majorityReached each time a
    * majority of nodes has succesfully been reached. After reaching majority once, the count is
    * reset and we attempt to reach majority again (we wait a bit before broadcasting again).
    * Terminates with a new state of follower if staleAfter has been reached or it detects its term
    * is expired.
    */
  def sendHeartbeats[F[_]](
    state:      NodeState,
    broadcast:  Stream[F, (NodeId, HeartbeatResponse)],
    staleAfter: FiniteDuration
  )(using F: Temporal[F], log: Logger[F]): Stream[F, NodeState] =
    broadcast.resettableTimeoutAccumulate(
      init      = Set(state.currentNode),
      timeout   = staleAfter,
      onTimeout = F.pure(state.transition(Role.Follower))
    ):
      case (externals, (external, HeartbeatResponse.Accepted)) =>
        val newExternals: Set[NodeId] =
          externals + external

        val info: F[Unit] =
          log.info("Heatbeats reached the cluster majority")

        if state.isExternalMajority(newExternals) then
          info as (Set(state.currentNode), ResettableTimeout.Reset())
        else
          F.pure(newExternals, ResettableTimeout.Skip())

      case (externals, (external, HeartbeatResponse.TermExpired(newTerm)))
          if state.isNotNew(newTerm) =>
        val warn: F[Unit] = log.warn:
          s"Got TermExpired with expired term $newTerm from $external"

        warn as (externals, ResettableTimeout.Skip())

      case (externals, (external, HeartbeatResponse.TermExpired(newTerm))) =>
        val warn: F[Unit] = log.warn:
          s"Current term expired, new term: $newTerm"

        warn as (externals, ResettableTimeout.Output(state.transition(Role.Follower, _ => newTerm)))

      case (_, (external, HeartbeatResponse.IllegalState(state))) =>
        F.raiseError(IllegalStateException(s"Node $external detected illegal state: $state"))

  end sendHeartbeats

  def attemptElection[F[_]](
    state:           NodeState,
    broadcast:       Stream[F, (NodeId, VoteResponse)],
    electionTimeout: FiniteDuration
  )(using F: Temporal[F], log: Logger[F]): Stream[F, NodeState] =
    broadcast.resettableTimeoutAccumulate(
      init      = Set(state.currentNode),
      timeout   = electionTimeout,
      onTimeout = F.pure(state.transition(Role.Candidate, _.next))
    ):
      case (externals, (external, VoteResponse.Granted)) =>
        val newExternals: Set[NodeId] =
          externals + external

        val info: F[Unit] =
          log.info(s"Won election")

        if state.isExternalMajority(newExternals) then
          info as (Set(state.currentNode), ResettableTimeout.Output(state.transition(Role.Leader)))
        else
          F.pure(newExternals, ResettableTimeout.Skip())

      case (externals, (external, VoteResponse.Rejected)) =>
        val info: F[Unit] =
          log.info(s"Vote rejected by $external")

        info as (externals, ResettableTimeout.Skip())

      case (externals, (external, VoteResponse.TermExpired(newTerm))) if state.isNotNew(newTerm) =>
        val warn: F[Unit] = log.warn:
          s"Got TermExpired with expired term $newTerm from $external"

        warn as (externals, ResettableTimeout.Skip())

      case (externals, (external, VoteResponse.TermExpired(newTerm))) =>
        val warn: F[Unit] = log.warn:
          s"Current term expired, new term: $newTerm"

        warn as (externals, ResettableTimeout.Output(state.transition(Role.Follower, _ => newTerm)))

      case (_, (external, VoteResponse.IllegalState(state))) =>
        F.raiseError(IllegalStateException(s"Node $external detected illegal state: $state"))
