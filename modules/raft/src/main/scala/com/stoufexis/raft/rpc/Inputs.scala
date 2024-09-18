package com.stoufexis.raft.rpc

import cats.effect.*
import cats.implicits.given
import fs2.*

import com.stoufexis.raft.model.Command

trait InputVotes[F[_]]:
  def incomingVotes: Stream[F, IncomingVote[F]]

trait InputAppends[F[_], In]:
  def incomingAppends: Stream[F, IncomingAppend[F, In]]

trait InputClients[F[_], In, Out, S]:
  def incomingClientRequests: Stream[F, IncomingClientRequest[F, In, Out, S]]

trait InputSource[F[_], In, Out, S]
    extends InputVotes[F]
    with InputAppends[F, In]
    with InputClients[F, In, Out, S]

trait InputSink[F[_], In, Out, S]:
  def requestVote(req: RequestVote): F[VoteResponse]

  def appendEntries(req: AppendEntries[In]): F[AppendResponse]

  def clientRequest(entry: Command[In]): F[ClientResponse[Out, S]]

/** Should handle retries. Should make sure that received messages are from nodes within the cluster, so only
  * known NodeIds.
  */
trait Inputs[F[_], In, Out, S] extends InputSource[F, In, Out, S] with InputSink[F, In, Out, S]

object Inputs:
  def apply[F[_]: Concurrent, In, Out, S](
    clientRequestsBuffer:  Int,
    incomingVotedBuffer:   Int,
    incomingAppendsBuffer: Int
  ): F[Inputs[F, In, Out, S]] =
    for
      vq <- RequestQueue[F, RequestVote, VoteResponse](incomingVotedBuffer)
      aq <- RequestQueue[F, AppendEntries[In], AppendResponse](incomingAppendsBuffer)
      cq <- RequestQueue[F, Command[In], ClientResponse[Out, S]](clientRequestsBuffer)
    yield new:
      def incomingVotes: Stream[F, IncomingVote[F]] =
        vq.consume.map(IncomingVote(_, _))

      def incomingAppends: Stream[F, IncomingAppend[F, In]] =
        aq.consume.map(IncomingAppend(_, _))

      def incomingClientRequests: Stream[F, IncomingClientRequest[F, In, Out, S]] =
        cq.consume.map(IncomingClientRequest(_, _))

      def requestVote(req: RequestVote): F[VoteResponse] =
        vq.offer(req)

      def appendEntries(req: AppendEntries[In]): F[AppendResponse] =
        aq.offer(req)

      def clientRequest(entry: Command[In]): F[ClientResponse[Out, S]] =
        cq.offer(entry)
