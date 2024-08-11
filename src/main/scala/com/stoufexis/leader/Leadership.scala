package com.stoufexis.leader

import cats.*
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

    def behaviorFor(f: (Term, NodeState) => Stream[F, (Term, NodeState)]): Stream[F, Unit]

  def logDropped[F[_]: Logger](str: String): F[Unit] =
    Logger[F].warn(s"Message dropped: $str")

  extension [F[_]: Monad, A](deferred: DeferredSink[F, A])
    def complete_(a: A): F[Unit] = deferred.complete(a).void

  def incoming[F[_]: Concurrent: Logger, A](rpc: RPC[F], state: State[F]): Stream[F, Unit] =
    (rpc.incomingHeartbeatRequests merge rpc.incomingVoteRequests).evalMap:
      case IncomingHeartbeat(request, sink) =>
        state.modify:
          case st @ (term, _) if request.term < term =>
            st -> sink.complete_(HeartbeatResponse.TermExpired(term))

          case st @ (term, NodeState.Leader) if request.term == term =>
            st -> logDropped(s"Multiple leaders for the same term $term")

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
            st -> sink.complete_(VoteResponse.TermExpired(term))

          case st @ (term, _) if request.term == term =>
            st -> logDropped(s"New election for current term $term")

          case _ =>
            (request.term, NodeState.VotedFollower(request.from))
              -> sink.complete_(VoteResponse.Granted)

  end incoming

  def interruptibleBackground[F[_]: Concurrent](stream: Stream[F, Unit]): Stream[F, F[Unit]] =
    for
      switch <- Stream.eval(Deferred[F, Unit])
      _      <- stream.interruptWhen(switch.get.attempt).spawn
    yield switch.complete(()).void

  def leaderBehavior[F[_]](
    rpc:           RPC[F],
    term:          Term,
    majorityCnt:   Int,
    nodeId:        NodeId,
    heartbeatRate: FiniteDuration,
    staleAfter:    FiniteDuration
  )(using
    F:   Temporal[F],
    log: Logger[F]
  ): Stream[F, (Term, NodeState)] =
    enum BroardcastResult:
      case Timeout
      case MajorityReached
      case TermExpired(newTerm: Term)

    // TODO: maybe detect missed periods
    val heartbeatSender: Stream[F, BroardcastResult] =
      (Stream.unit ++ Stream.fixedDelay(heartbeatRate)).flatMap: _ =>
        val responses: Stream[F, (NodeId, HeartbeatResponse)] =
          rpc.heartbeatBroadcast(HeartbeatRequest(nodeId, term))

        val timeoutAfterDetectStale: Stream[F, BroardcastResult.Timeout.type] =
          Stream.sleep(staleAfter) as BroardcastResult.Timeout

        val broadcastResults: Stream[F, BroardcastResult] =
          responses
            .evalMapAccumulate(Set.empty[NodeId]):
              case (nodes, (node, HeartbeatResponse.Accepted)) =>
                val newNodes: Set[NodeId] =
                  nodes + node

                val output: Stream[F, BroardcastResult] =
                  if newNodes.size >= majorityCnt
                  then Stream(BroardcastResult.MajorityReached)
                  else Stream.empty

                log.debug(s"Node $node accepted heartbeat")
                  .as((newNodes, output))

              case (nodes, (node, HeartbeatResponse.TermExpired(newTerm))) =>
                if term >= newTerm then
                  val err: String =
                    s"Got TermExpired with non-new term from $node. current: $term, received: $newTerm"

                  logDropped(err) as (nodes, Stream.empty)
                else
                  F.pure(nodes, Stream(BroardcastResult.Timeout))
            .flatMap(_._2)

        (broadcastResults merge timeoutAfterDetectStale).take(1)

    heartbeatSender.flatMap:
      case BroardcastResult.Timeout =>
        Stream((term, NodeState.Follower))

      case BroardcastResult.TermExpired(newTerm) =>
        Stream((newTerm, NodeState.Follower))

      case BroardcastResult.MajorityReached =>
        Stream.empty
    .take(1)

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