package com.stoufexis.raft.statemachine

import cats.*
import cats.effect.kernel.*
import fs2.*

import com.stoufexis.raft.model.*
import com.stoufexis.raft.persist.*
import com.stoufexis.raft.rpc.*
import com.stoufexis.raft.typeclass.IntLike.*

import scala.collection.immutable.Queue

case class WaitingClient[F[_], S](
  startIdx: Index,
  endIdx:   Index,
  sink:     DeferredSink[F, ClientResponse[S]]
)

object WaitingClient:
  /** clients are assumed to be in order - the first to be dequeued has the lowest endIdx
    *
    * TODO: Test
    */
  extension [F[_], S](clients: Queue[WaitingClient[F, S]])
    def enqueue(startIdx: Index, endIdx: Index, sink: DeferredSink[F, ClientResponse[S]]) =
      clients.enqueue(WaitingClient(startIdx, endIdx, sink))

    def fulfill[A](commitIdx: Index, initS: S, automaton: (S, A) => S)(
      using
      F:   MonadThrow[F],
      C:   Compiler[F, F],
      log: Log[F, A]
    ): F[(Queue[WaitingClient[F, S]], S)] =
      def done(cl: Queue[WaitingClient[F, S]], s: S) =
        Pull.output1(cl, s) >> Pull.done

      def go(
        stream:  Stream[F, (Index, A)],
        acc:     S,
        head:    WaitingClient[F, S],
        clients: Queue[WaitingClient[F, S]]
      ): Pull[F, (Queue[WaitingClient[F, S]], S), Unit] =
        stream.pull.uncons1.flatMap:
          case None => done(clients, acc)

          case Some(((index, a), tail)) =>
            val newS: S = automaton(acc, a)

            if index >= head.endIdx then
              val nextClientsAcc: Queue[WaitingClient[F, S]] =
                clients.dequeue._2

              Pull.eval(head.sink.complete(ClientResponse.Executed(newS))) >> {
                nextClientsAcc.dequeueOption match
                  case Some((head, _)) => go(tail, newS, head, nextClientsAcc)
                  case None            => done(clients, newS)
              }
            else
              go(tail, newS, head, clients)

      clients.dequeueOption match
        case None =>
          F.pure(clients, initS)

        case Some((head, _)) =>
          go(log.rangeStream(head.startIdx, commitIdx), initS, head, clients)
            .stream
            .compile
            .lastOrError
