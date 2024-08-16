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
    val foldDeps: Resource[F, (AckNack[F], FiniteDuration)] =
      for
        acks            <- confirmLeader.scopedAcks
        electionTimeout <- Resource.eval(timeout.nextElectionTimeout)
      yield (acks, electionTimeout)

    def loop(nodeState: NodeState, term: Term): F[Nothing] =
      def fold(acks: AckNack[F], electionTimeout: FiniteDuration): F[(NodeState, Term)] =
        nodeState match
          case st @ NodeState.Leader =>
            val streams: List[Stream[F, (NodeState, Term)]] = List(
              leaderHandleHeartbeats(rpc.incomingHeartbeatRequests, term),
              leaderHandleVoteRequest(rpc.incomingVoteRequests, term),
              leaderHeartbeater(
                rpc,
                acks.ack,
                nodesInCluster,
                nodeId,
                heartbeatRate,
                staleAfter,
                term
              )
            )

            acks.ack >> streams.raceFirstOrError

          case st @ NodeState.Follower =>
            val streams: List[Stream[F, (NodeState, Term)]] = List(
              followerHandleHeartbeats(rpc.incomingHeartbeatRequests, term, electionTimeout),
              followerHandleVoteRequests(rpc.incomingVoteRequests, term)
            )

            acks.nack >> streams.raceFirstOrError

      end fold

      foldDeps.use(fold) >>= loop

    end loop

    loop(NodeState.Follower, Term.init)

  def leaderHandleHeartbeats[F[_]](
    incoming: Stream[F, IncomingHeartbeat[F]],
    term:     Term
  )(using F: MonadThrow[F], log: Logger[F], c: Compiler[F, F]): Stream[F, (NodeState, Term)] =
    incoming.evalMapFilter:
      case IncomingHeartbeat(request, sink) if request.term < term =>
        for
          _ <- sink.complete_(HeartbeatResponse.TermExpired(term))
          _ <- Logger[F].warn(s"Detected stale leader ${request.from}")
        yield None

      case IncomingHeartbeat(request, sink) if request.term == term =>
        val state: String =
          s"Duplicate leaders for term $term"

        for
          _ <- sink.complete_(HeartbeatResponse.IllegalState(state))
          _ <- F.raiseError(IllegalStateException(state))
        yield None

      case IncomingHeartbeat(request, sink) =>
        for
          _ <- sink.complete_(HeartbeatResponse.Accepted)
          _ <- Logger[F].info(s"New leader ${request.from} accepted for term ${request.term}")
        yield Some(NodeState.Follower, request.term)

  def leaderHandleVoteRequest[F[_]](
    incoming: Stream[F, IncomingVoteRequest[F]],
    term:     Term
  )(using F: MonadThrow[F], log: Logger[F], c: Compiler[F, F]): Stream[F, (NodeState, Term)] =
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

  def followerHandleHeartbeats[F[_]](
    incoming:        Stream[F, IncomingHeartbeat[F]],
    term:            Term,
    electionTimeout: FiniteDuration
  )(using
    F:   Temporal[F],
    log: Logger[F]
  ): Stream[F, (NodeState, Term)] =
    incoming.resettableTimeout(
      timeout   = electionTimeout,
      onTimeout = (NodeState.Candidate, term.next)
    ):
      case IncomingHeartbeat(request, sink) if request.term < term =>
        for
          _ <- sink.complete_(HeartbeatResponse.TermExpired(term))
          _ <- Logger[F].warn(s"Detected stale leader ${request.from}")
        yield ResettableTimeout.Skip()

      case IncomingHeartbeat(request, sink) if request.term == term =>
        for
          _ <- sink.complete_(HeartbeatResponse.Accepted)
          _ <- Logger[F].debug(s"Heartbeat accepted from ${request.from}")
        yield ResettableTimeout.Reset()

      case IncomingHeartbeat(request, sink) =>
        for
          _ <- sink.complete_(HeartbeatResponse.Accepted)
          _ <- Logger[F].info(s"New leader ${request.from} accepted for term ${request.term}")
        yield ResettableTimeout.Output(NodeState.Follower, request.term)

  def followerHandleVoteRequests[F[_]](
    incoming: Stream[F, IncomingVoteRequest[F]],
    term:     Term
  )(using F: MonadThrow[F], log: Logger[F], c: Compiler[F, F]): Stream[F, (NodeState, Term)] =
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

  def leaderHeartbeater[F[_]](
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
  ): Stream[F, (NodeState, Term)] =
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

    /** Repeatedly broadcasts heartbeat requests to all other nodes. Calls majorityReached each time
      * a majority of nodes has succesfully been reached. After reaching majority once, the count is
      * reset and we attempt to reach majority again (we wait a bit before broadcasting again).
      * Terminates with a new state of follower if staleAfter has been reached or it detects its
      * term is expired.
      */
    broadcastHeartbeat.resettableTimeoutAccumulate(
      init      = Set(currentNodeId),
      timeout   = staleAfter,
      onTimeout = (NodeState.Follower, term)
    ):
      case (nodes, (node, HeartbeatResponse.Accepted)) =>
        val newNodes: Set[NodeId] =
          nodes + node

        if newNodes.size >= (nodesInCluster.size / 2 + 1) then
          majorityReached as (Set(currentNodeId), ResettableTimeout.Reset())
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

  end leaderHeartbeater
