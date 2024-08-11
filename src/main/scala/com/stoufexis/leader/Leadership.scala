package com.stoufexis.leader

import cats.Functor
import cats.MonadThrow
import cats.effect.kernel.*
import cats.implicits.given
import com.stoufexis.leader.model.*
import com.stoufexis.leader.rpc.*
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
  // def apply[F[_], A](using rpc: RPC[F, A], timeout: Timeout[F]): Leadership[F] = ???

  // val  =
  //

  trait State[F[_]]:
    val nodeId: NodeId

    def modify[B](f: (Term, NodeState) => ((Term, NodeState), F[B])): F[B]

    def changes: Stream[F, (Term, NodeState)]

  def illegalState[F[_]: MonadThrow, A](str: String): F[A] =
    MonadThrow[F].raiseError(IllegalStateException(str))

  def incoming[F[_]: Concurrent, A](rpc: RPC[F], state: State[F]): Stream[F, Boolean] =
    (rpc.incomingHeartbeatRequests merge rpc.incomingVoteRequests).evalMap:
      case IncomingHeartbeat(request, sink) =>
        state.modify:
          case st @ (term, _) if request.term < term =>
            st -> sink.complete(HeartbeatResponse.TermExpired(term))

          case st @ (term, NodeState.Leader) if request.term == term =>
            st -> illegalState(s"Multiple leaders for the same term $term")

          // If a node from the same or greater term claims to be the leader, we recognize it
          // and adopt its term
          case _ =>
            (request.term, NodeState.Follower) -> sink.complete(HeartbeatResponse.Accepted)

      case IncomingVoteRequest(request, sink) =>
        state.modify:
          case st @ (term, _) if request.term < term =>
            st -> sink.complete(VoteResponse.TermExpired(term))

          // This node has voted for its self or another node in this term
          case st @ (term, NodeState.Candidate | NodeState.VotedFollower) if request.term == term =>
            st -> sink.complete(VoteResponse.TermExpired(term))

          case st @ (term, _) if request.term == term =>
            st -> illegalState(s"New election for current term $term")

          case _ =>
            (request.term, NodeState.VotedFollower(request.from))
              -> sink.complete(VoteResponse.Granted)

  end incoming

  def interruptibleBackground[F[_]: Concurrent](
    stream: Stream[F, Unit]
  ): Stream[F, Deferred[F, Unit]] =
    for
      switch <- Stream.eval(Deferred[F, Unit])
      _      <- stream.interruptWhen(switch.get.attempt).spawn
    yield switch

  def notifier[F[_]: Temporal: Logger](
    every: FiniteDuration,
    rpc:   RPC[F],
    state: State[F]
  ) =
    val handleHeartbeatResponse: ((NodeId, HeartbeatResponse)) => F[Unit] =
      case (nodeId, HeartbeatResponse.Accepted) =>
        Logger[F].debug(s"Node $nodeId accepted heartbeat")

      case (nodeId, HeartbeatResponse.TermExpired(newTerm)) =>
        state.modify:
          case st @ (term, _) if term >= newTerm =>
            val err: String =
              s"Got TermExpired with non-new term from $nodeId. current: $term, received: $newTerm"

            st -> illegalState(err)

          case _ =>
            (newTerm, NodeState.Follower) -> Temporal[F].unit

    def onTransitionToLeader(term: Term): Stream[F, Unit] =
      // TODO: maybe detect missed periods
      Stream
        .fixedRateStartImmediately(every)
        .flatMap: _ =>
          rpc.heartbeatBroadcast(HeartbeatRequest(state.nodeId, term))
            .evalMap(handleHeartbeatResponse)

    def onTransitionToCandidate(term: Term): Stream[F, Unit] =
      ???

    state.changes
      .flatMap:
        case (term, NodeState.Leader)    => interruptibleBackground(onTransitionToLeader(term))
        case (term, NodeState.Candidate) => interruptibleBackground(onTransitionToCandidate(term))
        case _                           => Stream.empty
      .zipWithNext
      .evalMap((previous, _) => previous.complete(()))

  // val inputs: Stream[F, IncomingHeartbeat[F] | IncomingVoteRequest[F]] =
  //   rpc.incomingHeartbeatRequests merge rpc.incomingVoteRequests

  // inputs.evalMap:
  //   case IncomingHeartbeat(request, sink) => ???

  //   case IncomingVoteRequest(request, sink) => ???

  // for
  //   inVotes

  // yield ()
