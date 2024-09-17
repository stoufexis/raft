package com.stoufexis.raft.statemachine

import cats.*
import cats.effect.kernel.*
import cats.implicits.given
import fs2.*
import org.typelevel.log4cats.Logger

import com.stoufexis.raft.*
import com.stoufexis.raft.model.*
import com.stoufexis.raft.rpc.*

object Candidate:
  def apply[F[_]: Logger: Temporal, In, S](state: NodeInfo)(using
    cfg: Config[F, In, ?, S]
  ): Behaviors[F] =
    Behaviors(
      appends(state),
      inVotes(state),
      cfg.inputs.incomingClientRequests.respondWithLeader(None),
      solicitVotes(state)
    )

  def appends[F[_]: Logger: MonadThrow, In, S](state: NodeInfo)(using
    inputs: InputSource[F, In, ?, S]
  ): Stream[F, NodeInfo] =
    inputs.incomingAppends.evalMapFirstSome:
      case IncomingAppend(req, sink) if state.isExpired(req.term) =>
        req.termExpired(state, sink) as None

      /** Someone else got elected before us. Recognise them. Append will be handled when transitioned to
        * follower. Incoming term may be current or a larger one, we always transition to follower.
        */
      case IncomingAppend(req, sink) =>
        Some(state.toFollower(req.term, req.leaderId)).pure[F]

  def inVotes[F[_]: Logger: MonadThrow, In, S](state: NodeInfo)(using
    inputs: InputSource[F, In, ?, S]
  ): Stream[F, NodeInfo] =
    inputs.incomingVotes.evalMapFirstSome:
      case IncomingVote(req, sink) if state.isExpired(req.term) =>
        req.termExpired(state, sink) as None

      /** We have voted for ourselves this term, as we are a candidate.
        */
      case IncomingVote(req, sink) if state.isCurrent(req.term) =>
        req.reject(sink) as None

      /** Vote request for next term. Our term has expired, transition to follower. When we are in a follower
        * status, we can evaluate if this candidate is fit to be leader, by gauging how up to date its log is.
        */
      case IncomingVote(req, sink) =>
        Some(state.toFollowerUnknownLeader(req.term)).pure[F]

  /** No appends happen in this state, so we can always use the last idx and term found in the log for the
    * election.
    */
  def solicitVotes[F[_], In, S](state: NodeInfo)(using
    F:      Temporal[F],
    log:    Logger[F],
    config: Config[F, In, ?, S]
  ): Stream[F, NodeInfo] =
    import ResettableTimeout.*

    for
      electionTimeout <-
        Stream.eval(config.timeout.nextElectionTimeout)

      (lastLogTerm, lastLogIdx) <-
        Stream.eval(config.log.lastTermIndex.map(_.getOrElse(Term.uninitiated, Index.uninitiated)))

      out: NodeInfo <-
        def req(node: ExternalNode[F, In]): F[(NodeId, VoteResponse)] =
          node
            .requestVote(RequestVote(config.cluster.currentNode, state.term, lastLogIdx, lastLogTerm))
            .tupleLeft(node.id)

        Stream
          .iterable(config.cluster.otherNodes)
          .parEvalMapUnbounded(req)
          .resettableTimeoutAccumulate(
            init      = Set(config.cluster.currentNode),
            onTimeout = F.pure(state.toCandidateNextTerm),
            timeout   = electionTimeout
          ):
            case (nodes, (node, VoteResponse.Granted)) =>
              val newNodes: Set[NodeId] =
                nodes + node

              val voted: F[Unit] =
                log.info(s"Node $node granted vote")

              val success: F[Unit] =
                log.info(s"Majority votes collected")

              if config.cluster.isMajority(newNodes) then
                Output(success as state.toLeader)
              else
                Skip(voted as newNodes)

            case (nodes, (node, VoteResponse.Rejected)) =>
              Skip(log.info(s"Node $node rejected vote") as nodes)

            case (nodes, (_, VoteResponse.TermExpired(newTerm))) =>
              Output(log.warn(s"Detected stale term") as state.toFollowerUnknownLeader(newTerm))

            case (nodes, (_, VoteResponse.IllegalState(msg))) =>
              Skip(F.raiseError(IllegalStateException(msg)))
    yield out
