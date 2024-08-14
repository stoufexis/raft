package com.stoufexis.leader

import cats.*
import cats.effect.kernel.*
import cats.effect.std.Mutex
import cats.implicits.given
import com.stoufexis.leader.model.*
import com.stoufexis.leader.rpc.*
import com.stoufexis.leader.util.*
import fs2.*
import fs2.concurrent.SignallingRef
import org.typelevel.log4cats.Logger

import scala.concurrent.duration.FiniteDuration
import scala.math.Ordered.orderingToOrdered

import java.time.LocalDateTime

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
  val nodeId:         NodeId      = ???
  val nodesInCluster: Set[NodeId] = ???

  enum LeaderEvent derives CanEqual:
    case Timeout
    case TermExpired(newTerm: Term)
    case MajorityReached

  enum CandidateEvent derives CanEqual:
    case Unknown

  enum FollowerEvent derives CanEqual:
    case Unknown

  def incoming[F[_]: Concurrent: Logger, A](
    processConcurrency: Int,
    rpc:                RPC[F],
    state:              StateMachine.Update[F]
  ): Stream[F, Unit] =
    (rpc.incomingHeartbeatRequests merge rpc.incomingVoteRequests).parEvalMap(processConcurrency):
      case IncomingHeartbeat(request, sink) =>
        state.update:
          case st @ (term, _) if request.term < term =>
            st -> sink.complete_(HeartbeatResponse.TermExpired(term))

          case st @ (term, NodeState.Leader) if request.term == term =>
            st -> Logger[F].logDropped(s"Multiple leaders for the same term $term")

          // If a node from the same or greater term claims to be the leader, we recognize it
          // and adopt its term
          case _ =>
            (request.term, NodeState.Follower) -> sink.complete_(HeartbeatResponse.Accepted)

      case IncomingVoteRequest(request, sink) =>
        state.update:
          case st @ (term, _) if request.term < term =>
            st -> sink.complete_(VoteResponse.TermExpired(term))

          // This node has voted for its self or another node in this term
          case st @ (term, NodeState.Candidate | NodeState.VotedFollower) if request.term == term =>
            st -> sink.complete_(VoteResponse.Rejected)

          case st @ (term, _) if request.term == term =>
            st -> Logger[F].logDropped(s"New election for current term $term")

          case _ =>
            (request.term, NodeState.VotedFollower) -> sink.complete_(VoteResponse.Granted)

  end incoming

  def applyStateMachine[F[_]: Temporal: Logger](
    stateMachine:  StateMachine[F],
    rpc:           RPC[F],
    heartbeatRate: FiniteDuration,
    staleAfter:    FiniteDuration
  ): StateMachine.Update[F] =
    stateMachine:
      case (term, NodeState.Leader, majorityReached) =>
        leaderBehavior(rpc, nodesInCluster, nodeId, heartbeatRate, staleAfter, term)
          .flatMap:
            case LeaderEvent.MajorityReached      => Stream.exec(majorityReached.signal)
            case LeaderEvent.Timeout              => Stream((term, NodeState.Follower))
            case LeaderEvent.TermExpired(newTerm) => Stream((newTerm, NodeState.Follower))
          .compileFirstOrError

  def leaderBehavior[F[_]](
    rpc:            RPC[F],
    nodesInCluster: Set[NodeId],
    currentNodeId:  NodeId,
    heartbeatRate:  FiniteDuration,
    staleAfter:     FiniteDuration,
    term:           Term
  )(using
    F:   Temporal[F],
    log: Logger[F]
  ): Stream[F, LeaderEvent] =
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
    val majorityReachedOrTermExpired: Stream[F, LeaderEvent] =
      broadcastHeartbeat.evalMapFilterAccumulate(Set(currentNodeId)):
        case (nodes, (node, HeartbeatResponse.Accepted)) =>
          val newNodes: Set[NodeId] =
            nodes + node

          F.pure:
            if newNodes.size >= (nodesInCluster.size / 2 + 1)
            then (Set(currentNodeId), Some(LeaderEvent.MajorityReached))
            else (newNodes, None)

        case (nodes, (node, HeartbeatResponse.TermExpired(newTerm))) if term >= newTerm =>
          val warn: F[Unit] = log.logDropped:
            s"Got TermExpired with non-new term from $node. current: $term, received: $newTerm"

          warn as (nodes, None)

        case (nodes, (node, HeartbeatResponse.TermExpired(newTerm))) =>
          val warn: F[Unit] = log.warn:
            s"Detected expired term. previous: $term, new: $newTerm"

          warn as (nodes, Some(LeaderEvent.TermExpired(newTerm)))

    majorityReachedOrTermExpired.timeoutOnPullTo(staleAfter, Stream(LeaderEvent.Timeout))
