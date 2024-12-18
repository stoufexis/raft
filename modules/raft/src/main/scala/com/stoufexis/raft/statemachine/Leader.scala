package com.stoufexis.raft.statemachine

import cats.*
import cats.data.NonEmptySeq
import cats.effect.kernel.*
import cats.implicits.given
import fs2.*
import org.typelevel.log4cats.Logger

import com.stoufexis.raft.*
import com.stoufexis.raft.ExternalNode
import com.stoufexis.raft.model.*
import com.stoufexis.raft.persist.Log
import com.stoufexis.raft.rpc.*
import com.stoufexis.raft.typeclass.Empty
import com.stoufexis.raft.typeclass.IntLike.{*, given}

import scala.collection.immutable.Queue
import scala.concurrent.duration.FiniteDuration

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
  def apply[F[_], In, Out, S: Empty](state: NodeInfo)(using
    F:         Temporal[F],
    logger:    Logger[F],
    cluster:   Cluster[F, In],
    timeout:   ElectionTimeout[F],
    log:       Log[F, In],
    automaton: Automaton[In, Out, S],
    inputs:    InputSource[F, In, Out, S],
    bs:        AppenderCfg
  ): Resource[F, Behaviors[F]] =
    for
      // Closes the topics after a single NodeInfo is produced
      // Closing the topics interrupts subscribers and makes publishes no-ops
      // Any further writes or reads will return None
      newIdxs: BufferedTopic[F, Index] <-
        BufferedTopic[F, Index]

      matchIdxs: BufferedTopic[F, (NodeId, Index)] <-
        BufferedTopic[F, (NodeId, Index)]

      // Creating subscribers before the streams start running
      // so messages are enqueued if the subscriber hasnt started to pull yet
      appenders: List[Stream[F, NodeInfo]] <-
        cluster.otherNodes.toList.traverse: node =>
          newIdxs
            .newSubscriber
            .map(appender(state, node, _, matchIdxs))

      msub1: Stream[F, (NodeId, Index)] <-
        matchIdxs.newSubscriber

      msub2: Stream[F, (NodeId, Index)] <-
        matchIdxs.newSubscriber

      inputs: Behaviors[F] =
        Behaviors(
          appends(state),
          votes(state),
          partitionChecker(state, msub1),
          stateMachine(state, msub2, newIdxs)
        )
    yield inputs ++ appenders

  def votes[F[_]: MonadThrow: Logger](state: NodeInfo)(using inputs: InputVotes[F]): Stream[F, NodeInfo] =
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

  def appends[F[_]: MonadThrow: Logger, In](state: NodeInfo)(using
    inputs: InputAppends[F, In]
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

  def commitIdxFromMatch(matchIdxs: Map[NodeId, Index])(using c: Cluster[?, ?]): Option[Index] =
    matchIdxs
      .toVector
      .sortBy(_._2)(using Ordering[Index].reverse)
      .get(c.otherNodesSize / 2 - 1)
      .map(_._2)

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
  def stateMachine[F[_], In, Out, S: Empty](
    state:    NodeInfo,
    matchIdx: Stream[F, (NodeId, Index)],
    newIdxs:  BufferedPublisher[F, Index]
  )(using
    F:         Temporal[F],
    logger:    Logger[F],
    cluster:   Cluster[F, In],
    timeout:   ElectionTimeout[F],
    log:       Log[F, In],
    automaton: Automaton[In, Out, S],
    inputs:    InputClients[F, In, Out, S]
  ): Stream[F, Nothing] =
    for
      initIdx <- Stream.eval(log.lastTermIndex.map(_.fold(Index.uninitiated)(_._2)))
      _       <- Stream.eval(newIdxs.publish(initIdx))

      initS <-
        log
          .rangeStream(Index.uninitiated, initIdx)
          .fold(Empty[S].empty):
            case (s, (_, _, c)) => automaton(s, c.value)._1

      // Assumes that matchIdxs for each node always increase
      commitIdx: Stream[F, Index] =
        matchIdx
          .scan(Map.empty[NodeId, Index])(_ + _)
          // TODO: Unify the cluster majority related functions
          .mapFilter(commitIdxFromMatch)
          .increasing
          .dropping(1)
          .evalTap(cidx => logger.info(s"Commit index is now at $cidx"))

      commitsAndAppends: Stream[F, Either[Index, IncomingClientRequest[F, In, Out, S]]] =
        commitIdx.mergeEither(inputs.incomingClientRequests)

      // Waiting clients in the vector are assumed to have non-overlapping and continuous indexes
      acc: (Queue[WaitingClient[F, Out, S]], S) =
        (Queue.empty, initS)

      out: Nothing <-
        commitsAndAppends.evalScanDrain(acc):
          case ((clients, s), Left(commitIdx)) =>
            clients.fulfill(commitIdx, s, automaton(_, _))

          case (acc @ (clients, s), Right(req)) =>
            val ifNotExists: F[(Queue[WaitingClient[F, Out, S]], S)] =
              log
                .append(NonEmptySeq.of((state.term, req.entry)))
                .flatTap(newIdxs.publish(_))
                .map(i => (clients.enqueue(i, req.sink), s))

            val ifExists: F[(Queue[WaitingClient[F, Out, S]], S)] =
              req.sink.complete(ClientResponse.Skipped(s)) as acc

            log
              .commandIdExists(req.entry.id)
              .ifM(ifExists, ifNotExists)
    yield out

  /** If the majority of the cluster is not reached with any request for an electionTimeout, demote self to
    * follower. This makes sure that a partitioned leader will quickly stop acting as a leader, even if it
    * does not notice another leader with a higher term.
    */
  def partitionChecker[F[_], In](
    state:     NodeInfo,
    matchIdxs: Stream[F, (NodeId, Index)]
  )(using
    F:       Temporal[F],
    logger:  Logger[F],
    cluster: Cluster[F, In],
    timeout: ElectionTimeout[F],
    log:     Log[F, In]
  ): Stream[F, NodeInfo] =
    for
      electionTimeout: FiniteDuration <-
        Stream.eval(timeout.nextElectionTimeout)

      out: NodeInfo <-
        matchIdxs.resettableTimeoutAccumulate(
          init      = Set(cluster.currentNode),
          timeout   = electionTimeout,
          onTimeout = logger.info("timeout") as state.toFollowerUnknownLeader
        ):
          case (nodes, (node, _)) =>
            val newNodes: Set[NodeId] =
              nodes + node

            val info: F[Unit] =
              logger.debug("Cluster majority reached")

            val debug: F[Unit] =
              logger.trace(s"Received response from $node")

            // TODO: Unify the cluster majority related functions
            if cluster.isMajority(newNodes) then
              ResettableTimeout.Reset(info as Set(cluster.currentNode))
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
  def appender[F[_], In](
    state:     NodeInfo,
    node:      ExternalNode[F, In],
    newIdxs:   Stream[F, Index],
    matchIdxs: BufferedPublisher[F, (NodeId, Index)]
  )(using
    F:       Temporal[F],
    log:     Log[F, In],
    bs:      AppenderCfg,
    cluster: Cluster[F, In],
    logger:  Logger[F]
  ): Stream[F, NodeInfo] =
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
        val endIdx:   Index = startIdx + bs.appenderBatchSize

        val term: F[Term] =
          if matchIdx <= Index.uninitiated then
            F.pure(Term.uninitiated)
          else
            log.term(matchIdx)

        val info: F[(Term, Seq[(Term, Command[In])])] =
          term.product:
            if seek
            then F.pure(Seq.empty)
            else log.range(startIdx, endIdx)

        info.flatMap: (matchIdxTerm, entries) =>
          val request: AppendEntries[In] =
            AppendEntries(
              leaderId     = cluster.currentNode,
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

      go(matchIdx, newIdx, seek = false).map(_.some.separate)

    end send

    // assumes that elements in newIdxs are increasing
    newIdxs
      .evalTap(idx => logger.info(s"Appending $idx to ${node.id}"))
      .dropping(1)
      .repeatLast(bs.heartbeatEvery)
      .evalMapAccumulateFirstSome(Option.empty[Index])(send(_, _))
