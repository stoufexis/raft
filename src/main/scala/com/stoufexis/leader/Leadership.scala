package com.stoufexis.leader

import cats.*
import cats.effect.kernel.*
import cats.implicits.given
import fs2.*
import org.typelevel.log4cats.Logger

import com.stoufexis.leader.model.*
import com.stoufexis.leader.rpc.*
import com.stoufexis.leader.statemachine.*
import com.stoufexis.leader.statemachine.ConfirmLeader.AckNack
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

  enum Events derives CanEqual:
    case Timeout
    case NewState(state: NodeState, newTerm: Term)
    case MajorityReached

  def stateMachine[F[_]: Temporal: Logger](
    rpc:           RPC[F],
    confirmLeader: ConfirmLeader[F]
  ): F[Nothing] =
    def loop(nodeState: NodeState, term: Term): F[Nothing] =
      def fold(acks: AckNack[F]): F[(NodeState, Term)] = nodeState match
        case st @ NodeState.Leader =>
          acks.ack >> raceAll(
            handleIncomingHeartbeats(rpc, term, st),
            handleIncomingVote(rpc, term, st),
            heartbeater(rpc, acks.ack, nodesInCluster, nodeId, heartbeatRate, staleAfter, term)
          )

      confirmLeader.scopedAcks.use(fold) >>= loop

    end loop

    loop(NodeState.Follower, Term.init)

  def handleIncomingHeartbeats[F[_]](
    rpc:       RPC[F],
    term:      Term,
    nodeState: NodeState
  )(using
    F:   Temporal[F],
    log: Logger[F]
  ): F[(NodeState, Term)] =
    rpc.incomingHeartbeatRequests.compileEvalFirstSomeOrError:
      case IncomingHeartbeat(request, sink) =>
        nodeState match
          case _ if request.term < term =>
            for
              _ <- sink.complete_(HeartbeatResponse.TermExpired(term))
              _ <- Logger[F].warn(s"Detected stale leader ${request.from}")
            yield None

          case NodeState.Leader if request.term == term =>
            val state: String =
              s"Multiple leaders for the same term $term"

            for
              _ <- sink.complete_(HeartbeatResponse.IllegalState(state))
              _ <- F.raiseError(IllegalStateException(state))
            yield None

          case NodeState.Follower if request.term == term =>
            for
              _ <- sink.complete_(HeartbeatResponse.Accepted)
              _ <- Logger[F].debug(s"Heartbeat accepted from ${request.from}")
            yield None

          // If a node from the same or greater term claims to be the leader, we recognize it
          // and adopt its term
          case _ =>
            for
              _ <- sink.complete_(HeartbeatResponse.Accepted)
              _ <- Logger[F].info(s"Recognizing ${request.from} as the leader of ${request.term}")
            yield Some(NodeState.Follower, request.term)

  def handleIncomingVote[F[_]](
    rpc:       RPC[F],
    term:      Term,
    nodeState: NodeState
  )(using
    F:   Temporal[F],
    log: Logger[F]
  ): F[(NodeState, Term)] =
    rpc.incomingVoteRequests.compileEvalFirstSomeOrError:
      case IncomingVoteRequest(request, sink) =>
        nodeState match
          case _ if request.term < term =>
            for
              _ <- sink.complete_(VoteResponse.TermExpired(term))
              _ <- Logger[F].warn(s"Detected stale candidate ${request.from}")
            yield None

          // This node has voted for its self or another node in this term
          case NodeState.Candidate | NodeState.VotedFollower if request.term == term =>
            for
              _ <- sink.complete_(VoteResponse.Rejected)
              _ <- Logger[F].info(s"Rejecting vote request of ${request.from}")
            yield None

          case _ if request.term == term =>
            val state: String =
              s"New election for the current term ${term}"

            for
              _ <- sink.complete_(VoteResponse.IllegalState(state))
              _ <- F.raiseError(IllegalStateException(state))
            yield None

          case _ =>
            for
              _ <- sink.complete_(VoteResponse.Granted)
              _ <- Logger[F].info(s"Voted for ${request.from} in term ${request.term}")
            yield Some(NodeState.VotedFollower, request.term)

  def heartbeater[F[_]](
    rpc:             RPC[F],
    majorityReached: F[Unit],
    nodesInCluster:  Set[NodeId],
    currentNodeId:   NodeId,
    heartbeatRate:   FiniteDuration,
    staleAfter:      FiniteDuration,
    term:            Term
  )(using
    F:   Temporal[F],
    log: Logger[F]
  ): F[(NodeState, Term)] =
    // TODO: handle rpc errors
    def repeatHeartbeat(to: NodeId): Stream[F, (NodeId, HeartbeatResponse)] =
      repeatOnInterval(
        heartbeatRate,
        rpc.heartbeatRequest(to, HeartbeatRequest(currentNodeId, term)).map((to, _))
      )

    val broadcastHeartbeat: Stream[F, (NodeId, HeartbeatResponse)] =
      Stream
        .iterable(nodesInCluster - currentNodeId)
        .map(repeatHeartbeat)
        .parJoinUnbounded

    // Repeatedly broadcasts heartbeat requests to all other nodes
    // Emits MajorityReached each time a majority of nodes has succesfully been reached.
    // After reaching majority once, the count is reset and we attempt to reach majority again (we wait a bit before broadcasting again).
    // Emits TermExpired if it detects a new term from one of the other nodes
    val majorityReachedOrTermExpired: Stream[F, (NodeState, Term)] =
      broadcastHeartbeat.evalMapFilterAccumulate(Set(currentNodeId)):
        case (nodes, (node, HeartbeatResponse.Accepted)) =>
          val newNodes: Set[NodeId] =
            nodes + node

          if newNodes.size >= (nodesInCluster.size / 2 + 1) then
            majorityReached as (Set(currentNodeId), None)
          else
            F.pure(newNodes, None)

        case (nodes, (node, HeartbeatResponse.TermExpired(newTerm))) if term >= newTerm =>
          val warn: F[Unit] = log.logDropped:
            s"Got TermExpired with non-new term from $node. current: $term, received: $newTerm"

          warn as (nodes, None)

        case (nodes, (node, HeartbeatResponse.TermExpired(newTerm))) =>
          val warn: F[Unit] = log.warn:
            s"Detected expired term. previous: $term, new: $newTerm"

          warn as (nodes, Some(NodeState.Follower, newTerm))

        case (_, (node, HeartbeatResponse.IllegalState(state))) =>
          F.raiseError(IllegalStateException(s"Node $node detected illegal state: $state"))

    majorityReachedOrTermExpired
      .compile
      .firstOrTimeout((NodeState.Follower, term))

  end heartbeater
