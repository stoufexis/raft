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
import scala.math.Ordered.orderingToOrdered

/** Raft leadership for a single entity
  */
trait Leadership[F[_]]:
  def currentTerm: F[Term]

  def currentState: F[NodeState]

  def termStream: Stream[F, Term]

  def stateStream: Stream[F, NodeState]

  def discoverLeader: F[Discovery]

  def waitUntilConfirmedLeader: F[Boolean]

// TODO: general error handling
object Leadership:

  def stateMachine[F[_]: Async](
    rpc:           RPC[F],
    timeout:       Timeout[F],
    nodes:         Nodes,
    heartbeatRate: FiniteDuration,
    voteRate:      FiniteDuration,
    staleAfter:    FiniteDuration
  ): F[Nothing] =
    def loop(nodeState: NodeState, term: Term): F[Nothing] =
      val fold: F[(NodeState, Term)] =
        for
          given Logger[F] <-
            Slf4jLogger.fromName[F](nodeState.print(term))

          electionTimeout: FiniteDuration <-
            timeout.nextElectionTimeout

          output <-
            def behavior(extra: Stream[F, (NodeState, Term)]*): F[(NodeState, Term)] =
              raceFirstOrError:
                handleHeartbeats(nodeState, term, electionTimeout, rpc.incomingHeartbeatRequests) ::
                handleVoteRequests(nodeState, term, rpc.incomingVoteRequests) ::
                extra.toList

            nodeState match
              case NodeState.Leader =>
                val broadcast: Stream[F, (NodeId, HeartbeatResponse)] =
                  rpc.broadcastHeartbeat(term, nodes, heartbeatRate)

                behavior:
                  sendHeartbeats(term, broadcast, nodes, staleAfter)

              case NodeState.Follower =>
                behavior()

              case NodeState.VotedFollower =>
                behavior()

              case NodeState.Candidate =>
                val broadcast: Stream[F, (NodeId, VoteResponse)] =
                  rpc.broadcastVote(term, nodes, heartbeatRate)

                behavior:
                  attemptElection(term, broadcast, nodes, staleAfter)
        yield output

      fold >>= loop
    end loop

    loop(NodeState.Follower, Term.init)
  end stateMachine

  def handleHeartbeats[F[_]](
    nodeState:       NodeState,
    term:            Term,
    electionTimeout: FiniteDuration,
    incoming:        Stream[F, IncomingHeartbeat[F]]
  )(using F: Temporal[F], log: Logger[F]): Stream[F, (NodeState, Term)] =
    nodeState match
      case NodeState.Leader =>
        incoming.evalMapFilter:
          case IncomingHeartbeat(request, sink) if request.term < term =>
            for
              _ <- sink.complete_(HeartbeatResponse.TermExpired(term))
              _ <- log.warn(s"Detected stale leader ${request.from}")
            yield None

          case IncomingHeartbeat(request, sink) if request.term == term =>
            val state: String = s"Duplicate leaders for term"

            for
              _ <- sink.complete_(HeartbeatResponse.IllegalState(state))
              _ <- F.raiseError(IllegalStateException(state))
            yield None

          case IncomingHeartbeat(request, sink) =>
            for
              _ <- sink.complete_(HeartbeatResponse.Accepted)
              _ <- log.info(s"New leader ${request.from} accepted for term ${request.term}")
            yield Some(NodeState.Follower, request.term)

      case NodeState.Follower | NodeState.VotedFollower =>
        incoming.resettableTimeout(
          timeout   = electionTimeout,
          onTimeout = F.pure(NodeState.Candidate, term.next)
        ):
          case IncomingHeartbeat(request, sink) if request.term < term =>
            for
              _ <- sink.complete_(HeartbeatResponse.TermExpired(term))
              _ <- log.warn(s"Detected stale leader ${request.from}")
            yield ResettableTimeout.Skip()

          case IncomingHeartbeat(request, sink) if request.term == term =>
            for
              _ <- sink.complete_(HeartbeatResponse.Accepted)
              _ <- log.debug(s"Heartbeat accepted from ${request.from}")
            yield ResettableTimeout.Reset()

          case IncomingHeartbeat(request, sink) =>
            for
              _ <- sink.complete_(HeartbeatResponse.Accepted)
              _ <- log.info(s"New leader ${request.from} accepted for term ${request.term}")
            yield ResettableTimeout.Output(NodeState.Follower, request.term)

  def handleVoteRequests[F[_]](
    nodeState: NodeState,
    term:      Term,
    incoming:  Stream[F, IncomingVoteRequest[F]]
  )(using F: Temporal[F], log: Logger[F]): Stream[F, (NodeState, Term)] =
    nodeState match
      case NodeState.Leader =>
        incoming.evalMapFilter:
          case IncomingVoteRequest(request, sink) if request.term < term =>
            for
              _ <- sink.complete_(VoteResponse.TermExpired(term))
              _ <- log.warn(s"Detected stale candidate ${request.from}")
            yield None

          case IncomingVoteRequest(request, sink) if request.term == term =>
            val state: String =
              s"New election for current term $term"

            for
              _ <- sink.complete_(VoteResponse.IllegalState(state))
              _ <- F.raiseError(IllegalStateException(state))
            yield None

          case IncomingVoteRequest(request, sink) =>
            for
              _ <- sink.complete_(VoteResponse.Granted)
              _ <- log.info(s"Voted for ${request.from} in term ${request.term}")
            yield Some(NodeState.VotedFollower, request.term)

      case NodeState.Follower =>
        incoming.evalMapFilter:
          case IncomingVoteRequest(request, sink) if request.term < term =>
            for
              _ <- sink.complete_(VoteResponse.TermExpired(term))
              _ <- log.warn(s"Detected stale candidate ${request.from}")
            yield None

          case IncomingVoteRequest(request, sink) if request.term == term =>
            val state: String =
              s"New election for current term $term"

            for
              _ <- sink.complete_(VoteResponse.IllegalState(state))
              _ <- F.raiseError(IllegalStateException(state))
            yield None

          case IncomingVoteRequest(request, sink) =>
            for
              _ <- sink.complete_(VoteResponse.Granted)
              _ <- log.info(s"Voted for ${request.from} in term ${request.term}")
            yield Some(NodeState.VotedFollower, request.term)

      case NodeState.VotedFollower =>
        incoming.evalMapFilter:
          case IncomingVoteRequest(request, sink) if request.term < term =>
            for
              _ <- sink.complete_(VoteResponse.TermExpired(term))
              _ <- log.warn(s"Detected stale candidate ${request.from}")
            yield None

          case IncomingVoteRequest(request, sink) if request.term == term =>
            val state: String =
              s"New election for current term $term"

            for
              _ <- sink.complete_(VoteResponse.IllegalState(state))
              _ <- F.raiseError(IllegalStateException(state))
            yield None

          case IncomingVoteRequest(request, sink) =>
            for
              _ <- sink.complete_(VoteResponse.Rejected)
              _ <- log.info(s"Rejected vote for ${request.from} in term ${request.term}")
            yield None

  /** Repeatedly broadcasts heartbeat requests to all other nodes. Calls majorityReached each time a
    * majority of nodes has succesfully been reached. After reaching majority once, the count is
    * reset and we attempt to reach majority again (we wait a bit before broadcasting again).
    * Terminates with a new state of follower if staleAfter has been reached or it detects its term
    * is expired.
    */
  def sendHeartbeats[F[_]](
    term:       Term,
    broadcast:  Stream[F, (NodeId, HeartbeatResponse)],
    nodes:      Nodes,
    staleAfter: FiniteDuration
  )(using F: Temporal[F], log: Logger[F]): Stream[F, (NodeState, Term)] =
    broadcast.resettableTimeoutAccumulate(
      init      = Set(nodes.currentNode),
      timeout   = staleAfter,
      onTimeout = F.pure(NodeState.Follower, term)
    ):
      case (externals, (external, HeartbeatResponse.Accepted)) =>
        val newExternals: Set[NodeId] =
          externals + external

        val info: F[Unit] =
          log.info("Heatbeats reached the cluster majority")

        if nodes.isExternalMajority(newExternals) then
          info as (Set(nodes.currentNode), ResettableTimeout.Reset())
        else
          F.pure(newExternals, ResettableTimeout.Skip())

      case (externals, (external, HeartbeatResponse.TermExpired(newTerm))) if term >= newTerm =>
        val warn: F[Unit] = log.warn:
          s"Got TermExpired with expired term $newTerm from $external"

        warn as (externals, ResettableTimeout.Skip())

      case (externals, (external, HeartbeatResponse.TermExpired(newTerm))) =>
        val warn: F[Unit] = log.warn:
          s"Current term expired, new term: $newTerm"

        warn as (externals, ResettableTimeout.Output(NodeState.Follower, newTerm))

      case (_, (external, HeartbeatResponse.IllegalState(state))) =>
        F.raiseError(IllegalStateException(s"Node $external detected illegal state: $state"))

  end sendHeartbeats

  def attemptElection[F[_]](
    term:            Term,
    broadcast:       Stream[F, (NodeId, VoteResponse)],
    nodes:           Nodes,
    electionTimeout: FiniteDuration
  )(using F: Temporal[F], log: Logger[F]) =
    broadcast.resettableTimeoutAccumulate(
      init      = Set(nodes.currentNode),
      timeout   = electionTimeout,
      onTimeout = F.pure(NodeState.Candidate, term.next)
    ):
      case (externals, (external, VoteResponse.Granted)) =>
        val newExternals: Set[NodeId] =
          externals + external

        val info: F[Unit] =
          log.info(s"Won election")

        if nodes.isExternalMajority(newExternals) then
          info as (Set(nodes.currentNode), ResettableTimeout.Output(NodeState.Leader, term))
        else
          F.pure(newExternals, ResettableTimeout.Skip())

      case (externals, (external, VoteResponse.Rejected)) =>
        val info: F[Unit] =
          log.info(s"Vote rejected by $external")

        info as (externals, ResettableTimeout.Skip())

      case (externals, (external, VoteResponse.TermExpired(newTerm))) =>
        val warn: F[Unit] = log.warn:
          s"Current term expired, new term: $newTerm"

        warn as (externals, ResettableTimeout.Output(NodeState.Follower, newTerm))

      case (_, (external, VoteResponse.IllegalState(state))) =>
        F.raiseError(IllegalStateException(s"Node $external detected illegal state: $state"))
