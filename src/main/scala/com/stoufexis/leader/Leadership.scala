package com.stoufexis.leader

import ConfirmLeader.AckNack
import cats.*
import cats.effect.kernel.*
import cats.implicits.given
import fs2.*
import org.typelevel.log4cats.Logger

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
  val nodeId:         NodeId         = ???
  val nodesInCluster: Set[NodeId]    = ???
  val heartbeatRate:  FiniteDuration = ???
  val staleAfter:     FiniteDuration = ???

  def stateMachine[F[_]: Temporal: Logger](
    rpc:           RPC[F],
    timeout:       Timeout[F],
    confirmLeader: ConfirmLeader[F]
  ): F[Nothing] =
    def loop(nodeState: NodeState, term: Term): F[Nothing] =
      val fold: F[(NodeState, Term)] =
        val resources: Resource[F, (AckNack[F], FiniteDuration)] =
          confirmLeader
            .scopedAcks
            .product(Resource.eval(timeout.nextElectionTimeout))

        resources.use: (acks, electionTimeout) =>
          def behavior(extra: Stream[F, (NodeState, Term)]*): F[(NodeState, Term)] =
            raceFirstOrError:
              handleHeartbeats(nodeState, term, electionTimeout, rpc.incomingHeartbeatRequests) ::
                handleVoteRequests(nodeState, term, rpc.incomingVoteRequests) ::
                extra.toList

          nodeState match
            case NodeState.Leader =>
              val broadcast: Stream[F, (NodeId, HeartbeatResponse)] =
                rpc.broadcastHeartbeat(term, nodesInCluster, nodeId, heartbeatRate)

              acks.ack >> behavior:
                sendHeartbeats(term, broadcast, nodeId, staleAfter, acks.ack)

            case NodeState.Follower =>
              acks.nack >> behavior()

            case NodeState.VotedFollower =>
              acks.nack >> behavior()

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
            val state: String = s"Duplicate leaders for term $term"

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
          onTimeout = (NodeState.Candidate, term.next)
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
    term:            Term,
    broadcast:       Stream[F, (NodeId, HeartbeatResponse)],
    nodeId:          NodeId,
    staleAfter:      FiniteDuration,
    majorityReached: F[Unit]
  )(using F: Temporal[F], log: Logger[F]): Stream[F, (NodeState, Term)] =
    broadcast.resettableTimeoutAccumulate(
      init      = Set(nodeId),
      timeout   = staleAfter,
      onTimeout = (NodeState.Follower, term)
    ):
      case (nodes, (node, HeartbeatResponse.Accepted)) =>
        val newNodes: Set[NodeId] = nodes + node

        if newNodes.size >= (nodesInCluster.size / 2 + 1) then
          majorityReached as (Set(nodeId), ResettableTimeout.Reset())
        else
          F.pure(newNodes, ResettableTimeout.Skip())

      case (nodes, (node, HeartbeatResponse.TermExpired(newTerm))) if term >= newTerm =>
        val warn: F[Unit] = log.warn:
          s"Got TermExpired with non-new term from $node. current: $term, received: $newTerm"

        warn as (nodes, ResettableTimeout.Skip())

      case (nodes, (node, HeartbeatResponse.TermExpired(newTerm))) =>
        val warn: F[Unit] = log.warn:
          s"Detected expired term. previous: $term, new: $newTerm"

        warn as (nodes, ResettableTimeout.Output(NodeState.Follower, newTerm))

      case (_, (node, HeartbeatResponse.IllegalState(state))) =>
        F.raiseError(IllegalStateException(s"Node $node detected illegal state: $state"))

  end sendHeartbeats
