package com.stoufexis.leader

import cats.*
import cats.effect.SyncIO
import cats.effect.kernel.*
import cats.implicits.given
import com.stoufexis.leader.model.*
import com.stoufexis.leader.rpc.*
import com.stoufexis.leader.util.Timeout
import fs2.*
import org.typelevel.log4cats.Logger

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

object Leadership:
  trait State[F[_]]:
    val nodeId: NodeId

    val majorityCnt: Int

    def update(f: (Term, NodeState) => (Term, NodeState)): F[Unit]

    def modify[B](f: (Term, NodeState) => ((Term, NodeState), F[B])): F[B]

    def behaviorFor(f: (Term, NodeState) => F[(Term, NodeState)]): Stream[F, Unit]

  extension [F[_]](l: Logger[F])
    def logDropped(str: String): F[Unit] =
      l.warn(s"Message dropped: $str")

  extension [F[_]: Monad, A](deferred: DeferredSink[F, A])
    def complete_(a: A): F[Unit] = deferred.complete(a).void

  def incoming[F[_]: Concurrent: Logger, A](
    processConcurrency: Int,
    rpc:                RPC[F],
    state:              State[F]
  ): Stream[F, Unit] =
    (rpc.incomingHeartbeatRequests merge rpc.incomingVoteRequests).parEvalMap(processConcurrency):
      case IncomingHeartbeat(request, sink) =>
        state.modify:
          case st @ (term, _) if request.term < term =>
            st -> sink.complete_(HeartbeatResponse.TermExpired(term))

          case st @ (term, NodeState.Leader) if request.term == term =>
            st -> Logger[F].logDropped(s"Multiple leaders for the same term $term")

          // If a node from the same or greater term claims to be the leader, we recognize it
          // and adopt its term
          case _ =>
            (request.term, NodeState.Follower) -> sink.complete_(HeartbeatResponse.Accepted)

      case IncomingVoteRequest(request, sink) =>
        state.modify:
          case st @ (term, _) if request.term < term =>
            st -> sink.complete_(VoteResponse.TermExpired(term))

          // This node has voted for its self or another node in this term
          case st @ (term, NodeState.Candidate | NodeState.VotedFollower) if request.term == term =>
            st -> sink.complete_(VoteResponse.Rejected)

          case st @ (term, _) if request.term == term =>
            st -> Logger[F].logDropped(s"New election for current term $term")

          case _ =>
            (request.term, NodeState.VotedFollower(request.from))
              -> sink.complete_(VoteResponse.Granted)

  end incoming

  def repeatOnInterval[F[_]: Temporal, A](
    delay:  FiniteDuration,
    stream: Stream[F, A]
  ): Stream[F, A] =
    (Stream.unit ++ Stream.fixedDelay(delay)) >> stream

  def leaderBehavior[F[_]: Temporal: Logger](
    rpc:           RPC[F],
    term:          Term,
    majorityCnt:   Int,
    nodeId:        NodeId,
    heartbeatRate: FiniteDuration,
    staleAfter:    FiniteDuration
  ): F[(Term, NodeState)] =
    enum BroardcastResult:
      case Timeout
      case MajorityReached
      case TermExpired(newTerm: Term)

    // TODO: maybe detect missed periods
    repeatOnInterval(heartbeatRate, rpc.heartbeatBroadcast(HeartbeatRequest(nodeId, term)))
      .evalMapAccumulate(Set.empty[NodeId]):
        case (nodes, (node, HeartbeatResponse.Accepted)) =>
          val newNodes: Set[NodeId] =
            nodes + node

          val output: Option[BroardcastResult] =
            if newNodes.size >= majorityCnt
            then Some(BroardcastResult.MajorityReached)
            else None

          Logger[F]
            .debug(s"Node $node accepted heartbeat")
            .as((newNodes, output))

        case (nodes, (node, HeartbeatResponse.TermExpired(newTerm))) =>
          if term >= newTerm then
            val err: String =
              s"Got TermExpired with non-new term from $node. current: $term, received: $newTerm"

            Logger[F].logDropped(err) as (nodes, None)
          else
            Temporal[F].pure(nodes, Some(BroardcastResult.TermExpired(newTerm)))
      .mapFilter(_._2)
      .timeoutOnPullTo(staleAfter, Stream(BroardcastResult.Timeout))
      .collectFirst:
        case BroardcastResult.Timeout              => (term, NodeState.Follower)
        case BroardcastResult.TermExpired(newTerm) => (newTerm, NodeState.Follower)
      .compile
      .lastOrError

  def notifier[F[_]: Temporal: Logger](
    rpc:     RPC[F],
    timeout: Timeout[F],
    state:   State[F]
  ): Stream[F, Unit] =
    state.behaviorFor:
      case (term, NodeState.Leader) =>
        leaderBehavior(
          rpc,
          term,
          state.majorityCnt,
          state.nodeId,
          timeout.heartbeatRate,
          timeout.assumeStaleAfter
        )
