package com.stoufexis.raft.statemachine

import cats.*
import cats.effect.kernel.*
import cats.implicits.given
import fs2.*
import org.typelevel.log4cats.Logger

import com.stoufexis.raft.model.*
import com.stoufexis.raft.rpc.*
import com.stoufexis.raft.typeclass.IntLike.*

import scala.collection.immutable.Queue
import scala.concurrent.duration.FiniteDuration

object Leader:

  /** Linearizability of reads is implemented quite inefficiently right now. A read is inserted as a
    * special entry in the log, and the read returns to the client only after the special entry is marked
    * as committed. Section "6.4 Processing read-only queries more efficiently" of the [Raft
    * paper](https://github.com/ongardie/dissertation#readme) explains a more efficient implementation.
    * Implementing the efficient algorithm is left as a future effort.
    *
    * This implementation of raft only updates the state machine in the leader node. Other nodes simply
    * replicate the log. The state machine in other nodes is reconstructed from the log if they become
    * the leader. TODO: I think this means I can get rid of the leaderCommit in AppendEntries requests
    */
  def apply[F[_], A, S: Monoid](
    state:          NodeInfo[S],
    heartbeatEvery: FiniteDuration,
    automaton:      (S, A) => S
  )(using
    F:       Temporal[F],
    log:     Log[F, A],
    rpc:     RPC[F, A, S],
    timeout: Timeout[F],
    logger:  Logger[F]
  ): Stream[F, NodeInfo[S]] =
    for
      // Closes the topics after a single NodeInfo[S] is produced
      // Closing the topics interrupts subscribers and makes publishes no-ops
      // Any further writes or reads will return None
      newIdxs: CloseableTopic[F, Index] <-
        Stream.resource(CloseableTopic[F, Index])

      matchIdxs: CloseableTopic[F, (NodeId, Index)] <-
        Stream.resource(CloseableTopic[F, (NodeId, Index)])

      (initIdx: Index, initState: S) <-
        log.readAll.fold((Index.init, Monoid[S].empty)):
          case ((_, s), (index, a)) => (index, automaton(s, a))

      electionTimeout: FiniteDuration <-
        Stream.eval(timeout.nextElectionTimeout)

      checker: Stream[F, NodeInfo[S]] =
        partitionChecker(state, matchIdxs, electionTimeout)

      sm: Stream[F, Nothing] =
        stateMachine(state, matchIdxs, newIdxs, initState, initIdx, automaton)

      appenders: List[Stream[F, NodeInfo[S]]] =
        state
          .allNodes
          .toList
          .map(appender(state, _, newIdxs, matchIdxs, heartbeatEvery))

      streams: List[Stream[F, NodeInfo[S]]] =
        handleIncomingAppends(state) :: handleIncomingVotes(state) :: checker :: sm :: appenders

      out: NodeInfo[S] <- raceFirst(streams)
    yield out

  def handleIncomingVotes[F[_], A, S](
    state: NodeInfo[S]
  )(using
    F:      MonadThrow[F],
    rpc:    RPC[F, A, S],
    logger: Logger[F]
  ): Stream[F, NodeInfo[S]] =
    rpc.incomingVotes.evalMapFirstSome:
      case IncomingVote(req, sink) if state.isExpired(req.term) =>
        req.termExpired(state, sink) as None

      case IncomingVote(req, sink) if state.isCurrent(req.term) =>
        req.reject(sink) as None

      /** This leader is being superseeded. As with the handleAppends transition, wait until the
        * transition has happened to grant.
        */
      case IncomingVote(req, sink) =>
        F.pure(Some(state.toFollowerUnknownLeader(req.term)))

  def handleIncomingAppends[F[_], A, S](
    state: NodeInfo[S]
  )(using
    F:      MonadThrow[F],
    rpc:    RPC[F, A, S],
    logger: Logger[F]
  ): Stream[F, NodeInfo[S]] =
    rpc.incomingAppends.evalMapFirstSome:
      case IncomingAppend(req, sink) if state.isExpired(req.term) =>
        req.termExpired(state, sink) as None

      case IncomingAppend(req, sink) if state.isCurrent(req.term) =>
        req.duplicateLeaders(sink) // throws error, it should impossible to reach this

      /** newer leader detected. Dont respond to the request, let it be consumed when we have
        * transitioned to follower. This ensures that the response happens only after this transition has
        * taken place. Otherwise, there is a chance that a different transition will happen instead even
        * after responding. This is a race condition caused by the parJoin of potentially
        * transition-producing streams.
        */
      case IncomingAppend(req, sink) =>
        F.pure(Some(state.toFollower(req.term, req.leaderId)))

  /** Receives client requests and fulfills them. Every request's entries are immediatelly appended to
    * the local log and we attempt to commit them. A commit is initiated by informing the appenders of a
    * new uncommitted index, via a publish to the newIdxs topic. The appenders attempt to append all
    * entries that are missing in the follower nodes, up to the new idx. Each time an index is confirmed
    * to be replicated in a follower node, a new event is emitted on the matchIdxs topic by the appender.
    * When the majority of the cluster (including the leader) has stored an entry of a given index, we
    * mark that index as committed.
    *
    * Whenever an index is marked as committed, we can fulfill any client request that appended entries
    * with a final index at or bellow the new committed index. Fulfilling happends by applying the
    * entries to the state machine and responding to the client with the new state.
    *
    * A bound on the number of concurrent waiting clients is not enforced here. Such a bound must be
    * enforced upstream, in incomingClientRequests.
    */
  def stateMachine[F[_], A, S](
    state:     NodeInfo[S],
    matchIdx:  CloseableTopic[F, (NodeId, Index)],
    newIdxs:   CloseableTopic[F, Index],
    initS:     S,
    initIdx:   Index,
    automaton: (S, A) => S
  )(using
    F:      Temporal[F],
    logger: Logger[F],
    rpc:    RPC[F, A, S],
    log:    Log[F, A]
  ): Stream[F, Nothing] =
    // start, end (inclusive), sink
    type WaitingClient = (Index, Index, DeferredSink[F, ClientResponse[S]])

    // clients are assumed to be in order here - the first to be dequeued is the oldest one
    def fulfill(clients: Queue[WaitingClient], cidx: Index, s: S): F[(Queue[WaitingClient], S)] =
      clients.dequeueOption match
        case Some(((start, end, sink), tail)) if end <= cidx =>
          log.range(start, end).flatMap: (_, entries) =>
            val newS = entries.foldLeft(s)(automaton)
            sink.complete(ClientResponse.Executed(newS)) >> fulfill(tail, cidx, newS)

        case _ => F.pure(clients, s)

    // Assumes that matchIdxs for each node always increase
    val commitIdx: Stream[F, Index] =
      matchIdx
        .subscribeUnbounded
        .scan(Map.empty[NodeId, Index])(_ + _)
        // TODO: Unify the cluster majority related functions
        .mapFilter(Pure.commitIdxFromMatch(state.otherNodes, _))
        .dropping(1)
        .evalTap(cidx => logger.debug(s"Commit index is now at $cidx"))

    val commitsAndAppends: Stream[F, Either[Index, IncomingClientRequest[F, A, S]]] =
      commitIdx.mergeEither(rpc.incomingClientRequests)

    val startup: Stream[F, Nothing] =
      Stream.exec(newIdxs.publish(initIdx).void)

    // Waiting clients in the vector are assumed to have non-overlapping and continuous indexes
    val acc: (Queue[WaitingClient], Index, S) =
      (Queue.empty, initIdx, initS)

    startup ++ commitsAndAppends.evalScanDrain(acc):
      case ((clients, idxEnd, s), Left(commitIdx)) =>
        fulfill(clients, commitIdx, s).map((_, idxEnd, _))

      case ((clients, idxEnd, s), Right(req)) =>
        for
          newIdx <- log.appendChunk(state.term, idxEnd, req.entries)
          _      <- newIdxs.publish(newIdx)
        yield (clients.enqueue(idxEnd + 1, newIdx, req.sink), newIdx, s)

  /** If the majority of the cluster is not reached with any request for an electionTimeout, demote self
    * to follower. This makes sure that a partitioned leader will quickly stop acting as a leader, even
    * if it does not notice another leader with a higher term.
    */
  def partitionChecker[F[_], S](
    state:           NodeInfo[S],
    matchIdxs:       CloseableTopic[F, (NodeId, Index)],
    electionTimeout: FiniteDuration
  )(using
    F:      Temporal[F],
    logger: Logger[F]
  ): Stream[F, NodeInfo[S]] =
    matchIdxs.subscribeUnbounded.resettableTimeoutAccumulate(
      init      = Set(state.currentNode),
      timeout   = electionTimeout,
      onTimeout = F.pure(state.toFollowerUnknownLeader)
    ):
      case (nodes, (node, _)) =>
        val newNodes: Set[NodeId] =
          nodes + node

        val info: F[Unit] =
          logger.info("Cluster majority reached, this node is still the leader")

        val debug: F[Unit] =
          logger.debug(s"Received response from $node")

        // TODO: Unify the cluster majority related functions
        if state.isMajority(newNodes) then
          (Set(state.currentNode), info, ResettableTimeout.Reset())
        else
          (newNodes, debug, ResettableTimeout.Skip())

  /** Maintains a matchIndex for each node, which points to the last index in the log for which every
    * preceeding entry matches, including the matchIndex entry.
    *
    * If there is a new uncommitted index, it attempts to send the uncommitted records to the node. If
    * the node returns a NotConsistent, we attempt to find the largest index for which the logs match, by
    * entering seek mode. Seek mode means repeatedly sending empty AppendEntries requests, each time
    * decreasing the matchIndex by 1, until an Accepted response is returned, which means we have found
    * the first log entry for which the logs match. After we have found that index, we exit seek mode and
    * attempt to replicate to the node all the leaders entries starting at that index, overwritting any
    * entries the node has that the leader does not agree with.
    *
    * If there is no new uncommitted index and heartbeatEvery time has passed, we emit a heartbeat, ie an
    * AppendEntries with no entries. The heartbeat may also fail the consistency check, in which case we
    * enter seek mode.
    *
    * If we encounter a TermExpired, we emit a new state, which signals the upstream for this stream's
    * termination. In all other cases nothing is emitted.
    *
    * Whenever a node responds to an rpc request, we know that we are not partitioned from it, so we
    * re-emit the latest matchIdx in the matchIdxs topic. This keeps the partition checker from timing
    * out.
    */
  def appender[F[_], A, S](
    state:          NodeInfo[S],
    node:           NodeId,
    newIdxs:        CloseableTopic[F, Index],
    matchIdxs:      CloseableTopic[F, (NodeId, Index)],
    heartbeatEvery: FiniteDuration
  )(using
    F:   Temporal[F],
    log: Log[F, A],
    rpc: RPC[F, A, S]
  ): Stream[F, NodeInfo[S]] =
    def send(matchIdxO: Option[Index], newIdx: Index): F[(Option[Index], Option[NodeInfo[S]])] =
      val matchIdx: Index =
        matchIdxO.getOrElse(newIdx)
      // Should be called whenever the node successfully responded, even if AppendEntries ultimately failed.
      // It keeps the partitionChecker from timing out.
      // Until we increment matchIdx, re-send the previously valid matchIdx
      def pinged(i: Index = matchIdx): F[Unit] =
        matchIdxs.publish((node, i)).void

      def go(matchIdx: Index, newIdx: Index, seek: Boolean = false): F[Either[Index, NodeInfo[S]]] =
        val info: F[(Term, Chunk[A])] =
          if seek then
            log.term(matchIdx).map((_, Chunk.empty))
          else
            log.range(matchIdx, newIdx)

        info.flatMap: (matchIdxTerm, entries) =>
          val request: AppendEntries[A] =
            AppendEntries(
              leaderId     = state.currentNode,
              term         = state.term,
              prevLogIndex = matchIdx,
              prevLogTerm  = matchIdxTerm,
              entries      = entries
            )

          rpc.appendEntries(node, request).flatMap:
            case AppendResponse.Accepted if seek =>
              pinged() >> go(matchIdx, newIdx, seek = false)

            case AppendResponse.Accepted =>
              pinged(newIdx) as Left(newIdx)

            case AppendResponse.NotConsistent =>
              pinged() >> go(matchIdx - 1, newIdx, seek = true)

            case AppendResponse.TermExpired(t) =>
              pinged() as Right(state.toFollowerUnknownLeader(t))

            case AppendResponse.IllegalState(msg) =>
              F.raiseError(IllegalStateException(msg))
      end go

      go(matchIdx, newIdx, seek = false)
        .map(_.some.separate)

    end send

    // assumes that elements in newIdxs are increasing
    // TODO: Make sure that if a NodeInfo is emitted no new sends can be made no matter how fast you pull
    newIdxs
      .subscribeUnbounded
      .dropping(1)
      .repeatLast(heartbeatEvery)
      .evalMapFilterAccumulate(Option.empty[Index])(send(_, _))
      .head
