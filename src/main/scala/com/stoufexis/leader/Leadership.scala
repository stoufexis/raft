package com.stoufexis.leader

import cats.MonadThrow
import cats.effect.kernel.*
import cats.implicits.given
import com.stoufexis.leader.model.*
import com.stoufexis.leader.rpc.*
import fs2.*

import scala.concurrent.duration.FiniteDuration
import scala.math.Ordered.orderingToOrdered
import cats.Functor

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
    def modify[B](f: (Term, NodeState) => ((Term, NodeState), F[B])): F[B]
    def changes: Stream[F, (Term, NodeState)]

  def illegalState[F[_]: MonadThrow, A](str: String): F[A] =
    MonadThrow[F].raiseError(IllegalStateException(str))

  def incoming[F[_]: Concurrent, A](rpc: RPC[F], state: State[F]): Stream[F, Boolean] =
    (rpc.incomingHeartbeatRequests merge rpc.incomingVoteRequests).evalMap:
      case IncomingHeartbeat(request, sink) =>
        state.modify:
          case st @ (term, _) if request.term < term =>
            st -> sink.complete(HeartbeatResponse.Rejected(term, "Term expired"))

          case st @ (term, NodeState.Leader) if request.term == term =>
            st -> illegalState(s"Multiple leaders for the same term $term")

          // If a node from the same or greater term claims to be the leader, we recognize it
          // and adopt its term
          case _ =>
            (request.term, NodeState.Follower) -> sink.complete(HeartbeatResponse.Accepted)

      case IncomingVoteRequest(request, sink) =>
        state.modify:
          case st @ (term, _) if request.term < term =>
            st -> sink.complete(VoteResponse.Rejected(term, "Term expired"))

          // This node has voted for its self or another node in this term
          case st @ (term, NodeState.Candidate | NodeState.VotedFollower) if request.term == term =>
            st -> sink.complete(VoteResponse.Rejected(term, "Already voted"))

          case st @ (term, _) if request.term == term =>
            st -> illegalState(s"New election for current term $term")

          case _ =>
            (request.term, NodeState.VotedFollower(request.from))
              -> sink.complete(VoteResponse.Granted)

  end incoming

  def sendHeartbeats[F[_]: Concurrent](every: FiniteDuration, state: State[F]) = ???
    // state.changes.map(_._2).flatMap:
    //   case NodeState.Leader =>
    //     Stream.fixedRate(every)

  // val inputs: Stream[F, IncomingHeartbeat[F] | IncomingVoteRequest[F]] =
  //   rpc.incomingHeartbeatRequests merge rpc.incomingVoteRequests

  // inputs.evalMap:
  //   case IncomingHeartbeat(request, sink) => ???

  //   case IncomingVoteRequest(request, sink) => ???

  // for
  //   inVotes

  // yield ()
