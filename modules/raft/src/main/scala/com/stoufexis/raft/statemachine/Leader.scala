package com.stoufexis.raft.statemachine

import cats.*
import cats.effect.kernel.*
import cats.implicits.given
import fs2.*
import org.typelevel.log4cats.Logger

import com.stoufexis.raft.*
import com.stoufexis.raft.ExternalNode
import com.stoufexis.raft.rpc.*
import com.stoufexis.raft.typeclass.IntLike.*

import scala.collection.immutable.Queue
import scala.concurrent.duration.FiniteDuration
import com.stoufexis.raft.model.*

object Leader:

  /** Linearizability of reads is implemented quite inefficiently right now. A read is inserted as a special
    * entry in the log, and the read returns to the client only after the special entry is marked as
    * committed. Section "6.4 Processing read-only queries more efficiently" of the [Raft
    * paper](https://github.com/ongardie/dissertation#readme) explains a more efficient implementation.
    * Implementing the efficient algorithm is left as a future effort.
    *
    * This implementation of raft only updates the state machine in the leader node. Other nodes simply
    * replicate the log. The state machine in other nodes is reconstructed from the log if they become the
    * leader. TODO: I think this means I can get rid of the leaderCommit in AppendEntries requests
    */
  def apply[F[_]: Temporal: Logger, A, S: Monoid](state: NodeInfo)(using
    config: Config[F, A, S]
  ): Resource[F, Behaviors[F]] =
    for
      // Closes the topics after a single NodeInfo is produced
      // Closing the topics interrupts subscribers and makes publishes no-ops
      // Any further writes or reads will return None
      newIdxs: CloseableTopic[F, Index] <-
        CloseableTopic[F, Index]

      matchIdxs: CloseableTopic[F, (NodeId, Index)] <-
        CloseableTopic[F, (NodeId, Index)]

      appenders: List[Stream[F, NodeInfo]] =
        config
          .cluster
          .otherNodes
          .toList
          .map(appender(state, _, newIdxs, matchIdxs, config))

      inputs: Behaviors[F] =
        Behaviors(
          appends(state),
          votes(state),
          partitionChecker(state, matchIdxs, config),
          stateMachine(state, matchIdxs, newIdxs)
        )
    yield inputs ++ appenders

  def votes[F[_]: MonadThrow: Logger, A, S](state: NodeInfo)(using
    inputs: InputSource[F, A, S]
  ): Stream[F, NodeInfo] =
    inputs.incomingVotes.evalMapFirstSome:
      case IncomingVote(req, sink) if state.isExpired(req.term) =>
        req.termExpired(state, sink) as None

      case IncomingVote(req, sink) if state.isCurrent(req.term) =>
        req.reject(sink) as None

      /** This leader is being superseeded. As with the handleAppends transition, wait until the transition
        * has happened to grant.
        */
      case IncomingVote(req, sink) =>
        Some(state.toFollowerUnknownLeader(req.term)).pure[F]

  def appends[F[_]: MonadThrow: Logger, A, S](state: NodeInfo)(using
    inputs: InputSource[F, A, S]
  ): Stream[F, NodeInfo] =
    inputs.incomingAppends.evalMapFirstSome:
      case IncomingAppend(req, sink) if state.isExpired(req.term) =>
        req.termExpired(state, sink) as None

      case IncomingAppend(req, sink) if state.isCurrent(req.term) =>
        req.duplicateLeaders(sink) // throws error, it should impossible to reach this

      /** newer leader detected. Dont respond to the request, let it be consumed when we have transitioned to
        * follower. This ensures that the response happens only after this transition has taken place.
        * Otherwise, there is a chance that a different transition will happen instead even after responding.
        * This is a race condition caused by the parJoin of potentially transition-producing streams.
        */
      case IncomingAppend(req, sink) =>
        Some(state.toFollower(req.term, req.leaderId)).pure[F]

  /** Receives client requests and fulfills them. Every request's entries are immediatelly appended to the
    * local log and we attempt to commit them. A commit is initiated by informing the appenders of a new
    * uncommitted index, via a publish to the newIdxs topic. The appenders attempt to append all entries that
    * are missing in the follower nodes, up to the new idx. Each time an index is confirmed to be replicated
    * in a follower node, a new event is emitted on the matchIdxs topic by the appender. When the majority of
    * the cluster (including the leader) has stored an entry of a given index, we mark that index as
    * committed.
    *
    * Whenever an index is marked as committed, we can fulfill any client request that appended entries with a
    * final index at or bellow the new committed index. Fulfilling happends by applying the entries to the
    * state machine and responding to the client with the new state.
    *
    * A bound on the number of concurrent waiting clients is not enforced here. Such a bound must be enforced
    * upstream, in incomingClientRequests.
    */
  def stateMachine[F[_], A, S: Monoid](
    state:    NodeInfo,
    matchIdx: CloseableTopic[F, (NodeId, Index)],
    newIdxs:  CloseableTopic[F, Index]
  )(using
    F:      Temporal[F],
    logger: Logger[F],
    config: Config[F, A, S]
  ): Stream[F, Nothing] =
    for
      initIdx <- Stream.eval(config.log.lastTermIndex.map(_._2))
      _       <- Stream.eval(newIdxs.publish(initIdx))

      (_, initS) <-
        config
          .log
          .rangeStream(Index.init, initIdx)
          .fold((Index.init, Monoid[S].empty)):
            case ((_, s), (index, a)) => (index, config.automaton(s, a))

      // Assumes that matchIdxs for each node always increase
      commitIdx: Stream[F, Index] =
        matchIdx
          .subscribeUnbounded
          .scan(Map.empty[NodeId, Index])(_ + _)
          // TODO: Unify the cluster majority related functions
          .mapFilter(Pure.commitIdxFromMatch(config.cluster.otherNodeIds, _))
          .dropping(1)
          .evalTap(cidx => logger.debug(s"Commit index is now at $cidx"))

      commitsAndAppends: Stream[F, Either[Index, IncomingClientRequest[F, A, S]]] =
        commitIdx.mergeEither(config.inputs.incomingClientRequests)

      // Waiting clients in the vector are assumed to have non-overlapping and continuous indexes
      acc: (Queue[WaitingClient[F, S]], Index, S) =
        (Queue.empty, initIdx, initS)

      out: Nothing <-
        commitsAndAppends.evalScanDrain(acc):
          case ((clients, idxEnd, s), Left(commitIdx)) =>
            clients
              .fulfill(commitIdx, s, config.automaton, config.log)
              .map((_, idxEnd, _))

          case ((clients, idxEnd, s), Right(req)) =>
            for
              newIdx <- config.log.appendChunk(state.term, idxEnd, req.entries)
              _      <- newIdxs.publish(newIdx)
            yield (clients.enqueue(idxEnd + 1, newIdx, req.sink), newIdx, s)
    yield out

  /** If the majority of the cluster is not reached with any request for an electionTimeout, demote self to
    * follower. This makes sure that a partitioned leader will quickly stop acting as a leader, even if it
    * does not notice another leader with a higher term.
    */
  def partitionChecker[F[_], A, S](
    state:     NodeInfo,
    matchIdxs: CloseableTopic[F, (NodeId, Index)],
    config:    Config[F, A, S]
  )(using
    F:      Temporal[F],
    logger: Logger[F]
  ): Stream[F, NodeInfo] =
    for
      electionTimeout: FiniteDuration <-
        Stream.eval(config.timeout.nextElectionTimeout)

      out: NodeInfo <-
        matchIdxs.subscribeUnbounded.resettableTimeoutAccumulate(
          init      = Set(config.cluster.currentNode),
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
            if config.cluster.isMajority(newNodes) then
              ResettableTimeout.Reset(info as Set(config.cluster.currentNode))
            else
              ResettableTimeout.Skip(debug as newNodes)
    yield out

  /** Maintains a matchIndex for each node, which points to the last index in the log for which every
    * preceeding entry matches, including the matchIndex entry.
    *
    * If there is a new uncommitted index, it attempts to send the uncommitted records to the node. If the
    * node returns a NotConsistent, we attempt to find the largest index for which the logs match, by entering
    * seek mode. Seek mode means repeatedly sending empty AppendEntries requests, each time decreasing the
    * matchIndex by 1, until an Accepted response is returned, which means we have found the first log entry
    * for which the logs match. After we have found that index, we exit seek mode and attempt to replicate to
    * the node all the leaders entries starting at that index, overwritting any entries the node has that the
    * leader does not agree with.
    *
    * If there is no new uncommitted index and heartbeatEvery time has passed, we emit a heartbeat, ie an
    * AppendEntries with no entries. The heartbeat may also fail the consistency check, in which case we enter
    * seek mode.
    *
    * If we encounter a TermExpired, we emit a new state, which signals the upstream for this stream's
    * termination. In all other cases nothing is emitted.
    *
    * Whenever a node responds to an rpc request, we know that we are not partitioned from it, so we re-emit
    * the latest matchIdx in the matchIdxs topic. This keeps the partition checker from timing out.
    */
  def appender[F[_], A, S](
    state:     NodeInfo,
    node:      ExternalNode[F, A, S],
    newIdxs:   CloseableTopic[F, Index],
    matchIdxs: CloseableTopic[F, (NodeId, Index)],
    config:    Config[F, A, S]
  )(using F: Temporal[F]): Stream[F, NodeInfo] =
    def send(matchIdxO: Option[Index], newIdx: Index): F[(Option[Index], Option[NodeInfo])] =
      val matchIdx: Index =
        matchIdxO.getOrElse(newIdx)
      // Should be called whenever the node successfully responded, even if AppendEntries ultimately failed.
      // It keeps the partitionChecker from timing out.
      // Until we increment matchIdx, re-send the previously valid matchIdx
      def pinged(i: Index = matchIdx): F[Unit] =
        matchIdxs.publish((node.id, i)).void

      def go(
        matchIdx: Index,
        newIdx:   Index,
        seek:     Boolean = false
      ): F[Either[Index, NodeInfo]] =
        val startIdx: Index = matchIdx + 1
        val endIdx:   Index = startIdx + config.appenderBatchSize

        val info: F[(Term, Chunk[A])] =
          config.log.term(matchIdx).product:
            if seek
            then F.pure(Chunk.empty)
            else config.log.range(startIdx, endIdx)

        info.flatMap: (matchIdxTerm, entries) =>
          val request: AppendEntries[A] =
            AppendEntries(
              leaderId     = config.cluster.currentNode,
              term         = state.term,
              prevLogIndex = matchIdx,
              prevLogTerm  = matchIdxTerm,
              entries      = entries
            )

          node.appendEntries(request).flatMap:
            case AppendResponse.Accepted if seek =>
              pinged() >> go(matchIdx, newIdx, seek = false)

            case AppendResponse.Accepted if endIdx >= newIdx =>
              pinged(newIdx) as Left(newIdx)

            case AppendResponse.Accepted =>
              pinged() >> go(endIdx, newIdx, seek = false)

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
      .repeatLast(config.heartbeatEvery)
      .evalMapAccumulateFirstSome(Option.empty[Index])(send(_, _))
