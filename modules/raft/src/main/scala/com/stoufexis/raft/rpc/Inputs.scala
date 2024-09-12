package com.stoufexis.raft.rpc

import cats.effect.*
import cats.implicits.given
import fs2.*

import com.stoufexis.raft.model.Command

trait InputSource[F[_], A, S]:
  def incomingVotes: Stream[F, IncomingVote[F]]

  def incomingAppends: Stream[F, IncomingAppend[F, A]]

  def incomingClientRequests: Stream[F, IncomingClientRequest[F, A, S]]

trait InputSink[F[_], A, S]:
  def requestVote(req: RequestVote): F[VoteResponse]

  def appendEntries(req: AppendEntries[A]): F[AppendResponse]

  def clientRequest(entry: Option[Command[A]]): F[ClientResponse[S]]

/** Should handle retries. Should make sure that received messages are from nodes within the cluster, so only
  * known NodeIds.
  */
trait Inputs[F[_], A, S] extends InputSource[F, A, S] with InputSink[F, A, S]

object Inputs:
  def apply[F[_]: Concurrent, A, S](
    clientRequestsBuffer:  Int,
    incomingVotedBuffer:   Int,
    incomingAppendsBuffer: Int
  ): F[Inputs[F, A, S]] =
    for
      vq <- RequestQueue[F, RequestVote, VoteResponse](incomingVotedBuffer)
      aq <- RequestQueue[F, AppendEntries[A], AppendResponse](incomingAppendsBuffer)
      cq <- RequestQueue[F, Option[Command[A]], ClientResponse[S]](clientRequestsBuffer)
    yield new:
      def incomingVotes: Stream[F, IncomingVote[F]] =
        vq.consume.map(IncomingVote(_, _))

      def incomingAppends: Stream[F, IncomingAppend[F, A]] =
        aq.consume.map(IncomingAppend(_, _))

      def incomingClientRequests: Stream[F, IncomingClientRequest[F, A, S]] =
        cq.consume.map(IncomingClientRequest(_, _))

      def requestVote(req: RequestVote): F[VoteResponse] =
        vq.offer(req)

      def appendEntries(req: AppendEntries[A]): F[AppendResponse] =
        aq.offer(req)

      def clientRequest(entry: Option[Command[A]]): F[ClientResponse[S]] =
        cq.offer(entry)
