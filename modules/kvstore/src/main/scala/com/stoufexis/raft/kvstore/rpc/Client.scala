package com.stoufexis.raft.kvstore.rpc

import cats.MonadThrow
import cats.implicits.given
import io.grpc.Metadata

import com.stoufexis.raft.ExternalNode
import com.stoufexis.raft.kvstore.proto
import com.stoufexis.raft.kvstore.proto.internal.raft.RaftFs2Grpc
import com.stoufexis.raft.kvstore.proto.public.OptionalString
import com.stoufexis.raft.kvstore.statemachine.KvCommand
import com.stoufexis.raft.model.*
import com.stoufexis.raft.rpc.*

object Client:
  def externalNode[F[_]](nodeId: NodeId, service: RaftFs2Grpc[F, Metadata])(using
    F: MonadThrow[F]
  ): ExternalNode[F, KvCommand] =
    new:
      val id: NodeId = nodeId

      def protoCmdMap(kvCommand: Command[KvCommand]): proto.public.Command =
        val id: String = kvCommand.id.string

        kvCommand.value match
          case KvCommand.Get(keys) =>
            proto.public.Get(id, keys.toSeq)

          case KvCommand.Update(sets) =>
            proto.public.Update(id, sets.fmap(OptionalString(_)))

          case KvCommand.TransactionUpdate(gets, sets) =>
            proto.public.TransactionUpdate(id, gets, sets.fmap(OptionalString(_)))

      def appendEntries(req: AppendEntries[KvCommand]): F[AppendResponse] =
        val res: F[proto.internal.raft.AppendResponse] =
          service.appendEntries(
            proto.internal.raft.AppendEntries(
              leaderId     = req.leaderId.string,
              term         = req.term.long,
              prevLogIndex = req.prevLogIndex.long,
              prevLogTerm  = req.prevLogTerm.long,
              entries      = req.entries.map(protoCmdMap)
            ),
            Metadata()
          )

        res.flatMap:
          case _: proto.internal.raft.AppendResponse.Empty.type =>
            F.raiseError(EmptyValueReceived)

          case proto.internal.raft.Accepted() =>
            F.pure(AppendResponse.Accepted)

          case proto.internal.raft.NotConsistent() =>
            F.pure(AppendResponse.Accepted)

          case proto.internal.raft.AppendTermExpired(term) =>
            F.pure(AppendResponse.TermExpired(Term(term)))

          case proto.internal.raft.AppendIllegalState(state) =>
            F.pure(AppendResponse.IllegalState(state))

      def requestVote(req: RequestVote): F[VoteResponse] =
        val res: F[proto.internal.raft.VoteResponse] =
          service.requestVote(
            proto.internal.raft.RequestVote(
              candidateId  = req.candidateId.string,
              term         = req.term.long,
              lastLogIndex = req.lastLogIndex.long,
              lastLogTerm  = req.lastLogTerm.long
            ),
            Metadata()
          )

        res.flatMap:
          case _: proto.internal.raft.VoteResponse.Empty.type => 
            F.raiseError(EmptyValueReceived)

          case proto.internal.raft.Granted() =>
            F.pure(VoteResponse.Granted)

          case proto.internal.raft.Rejected() =>
            F.pure(VoteResponse.Rejected)

          case proto.internal.raft.VoteTermExpired(term) =>
            F.pure(VoteResponse.TermExpired(Term(term)))

          case proto.internal.raft.VoteIllegalState(state) =>
            F.pure(VoteResponse.IllegalState(state))
        
