package com.stoufexis.raft.kvstore.rpc

import cats.*
import cats.implicits.given
import fs2.*
import io.grpc.Metadata

import com.stoufexis.raft.RaftNode
import com.stoufexis.raft.kvstore.proto
import com.stoufexis.raft.kvstore.statemachine.*
import com.stoufexis.raft.model.*
import com.stoufexis.raft.rpc.*

object Services:
  case object EmptyValueReceived extends RuntimeException("Received empty value")

  def protoCmdMap[F[_]](protoCmd: proto.public.Command)(using F: MonadThrow[F]): F[Command[KvCommand]] =
    import proto.public.CommandMessage.SealedValue

    protoCmd.asMessage.sealedValue match
      case SealedValue.Get(value) =>
        F.pure(Command(CommandId(value.id), KvCommand.Get(value.keys.toSet)))

      case SealedValue.Update(value) =>
        F.pure(Command(CommandId(value.id), KvCommand.Update(value.sets.fmap(_.value))))

      case SealedValue.TransactionUpdate(value) =>
        F.pure(Command(
          CommandId(value.id),
          KvCommand.TransactionUpdate(value.gets, value.sets.fmap(_.value))
        ))

      case _: SealedValue.Empty.type =>
        F.raiseError(EmptyValueReceived)

  def kvstore[F[_]: MonadThrow](raft: RaftNode[F, KvCommand, KvResponse, State])
    : proto.public.KVStoreFs2Grpc[F, Metadata] =
    new:
      def apply(request: proto.public.Command, ctx: Metadata): F[proto.public.Response] =
        protoCmdMap(request).flatMap(raft.clientRequest).map:
          case ClientResponse.Executed(_, KvResponse.Values(a)) =>
            proto.public.Values(a.fmap(proto.public.Value(_, _)))

          case ClientResponse.Executed(_, KvResponse.Success) => new proto.public.Success
          case ClientResponse.Skipped(_)                      => new proto.public.Skipped
          case ClientResponse.Redirect(leaderId)              => proto.public.Redirect(leaderId.string)
          case ClientResponse.UnknownLeader                   => new proto.public.UnknownLeader

  def raft[F[_]: MonadThrow](raft: RaftNode[F, KvCommand, KvResponse, State])
    : proto.internal.raft.RaftFs2Grpc[F, Metadata] =
    new:
      def requestVote(
        request: proto.internal.raft.RequestVote,
        ctx:     Metadata
      ): F[proto.internal.raft.VoteResponse] =
        val req: RequestVote =
          RequestVote(
            candidateId  = NodeId(request.candidateId),
            term         = Term(request.term),
            lastLogIndex = Index(request.lastLogIndex),
            lastLogTerm  = Term(request.lastLogTerm)
          )

        raft.requestVote(req).map:
          case VoteResponse.Granted             => new proto.internal.raft.Granted
          case VoteResponse.Rejected            => new proto.internal.raft.Rejected
          case VoteResponse.TermExpired(term)   => proto.internal.raft.VoteTermExpired(term.long)
          case VoteResponse.IllegalState(state) => proto.internal.raft.VoteIllegalState(state)

      def appendEntries(
        request: proto.internal.raft.AppendEntries,
        ctx:     Metadata
      ): F[proto.internal.raft.AppendResponse] =
        val cmds: F[Chunk[Command[KvCommand]]] =
          Chunk.from(request.entries).traverse(protoCmdMap)

        val req: F[AppendEntries[KvCommand]] = cmds.map: entries =>
          AppendEntries(
            leaderId     = NodeId(request.leaderId),
            term         = Term(request.term),
            prevLogIndex = Index(request.prevLogIndex),
            prevLogTerm  = Term(request.prevLogTerm),
            entries      = entries
          )

        req.flatMap(raft.appendEntries).map:
          case AppendResponse.Accepted             => new proto.internal.raft.Accepted
          case AppendResponse.NotConsistent        => new proto.internal.raft.Accepted
          case AppendResponse.TermExpired(newTerm) => proto.internal.raft.AppendTermExpired(newTerm.long)
          case AppendResponse.IllegalState(state)  => proto.internal.raft.AppendIllegalState(state)
