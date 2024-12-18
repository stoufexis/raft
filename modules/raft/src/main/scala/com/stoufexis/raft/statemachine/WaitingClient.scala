package com.stoufexis.raft.statemachine

import cats.*
import cats.effect.kernel.*
import fs2.*

import com.stoufexis.raft.model.*
import com.stoufexis.raft.persist.*
import com.stoufexis.raft.rpc.*
import com.stoufexis.raft.typeclass.IntLike.*

import scala.collection.immutable.Queue

case class WaitingClient[F[_], Out, S](
  idx:  Index,
  sink: DeferredSink[F, ClientResponse[Out, S]]
)

object WaitingClient:
  /** clients are assumed to be in order - the first to be dequeued has the lowest idx
    *
    * TODO: Test
    */
  extension [F[_], Out, S](clients: Queue[WaitingClient[F, Out, S]])
    def enqueue(idx: Index, sink: DeferredSink[F, ClientResponse[Out, S]]) =
      clients.enqueue(WaitingClient(idx, sink))

    def fulfill[In](commitIdx: Index, initS: S, automaton: (S, In) => (S, Out))(
      using
      log: Log[F, In],
      F:   MonadThrow[F],
      C:   Compiler[F, F]
    ): F[(Queue[WaitingClient[F, Out, S]], S)] =
      def done(cl: Queue[WaitingClient[F, Out, S]], s: S) =
        Pull.output1(cl, s) >> Pull.done

      def go(
        stream:  Stream[F, (Index, Term, Command[In])],
        acc:     S,
        head:    WaitingClient[F, Out, S],
        clients: Queue[WaitingClient[F, Out, S]]
      ): Pull[F, (Queue[WaitingClient[F, Out, S]], S), Unit] =
        stream.pull.uncons1.flatMap:
          case None => done(clients, acc)

          case Some(((index, _, e), tail)) =>
            val (newS, out) = automaton(acc, e.value)

            if index >= head.idx then
              val nextClientsAcc: Queue[WaitingClient[F, Out, S]] =
                clients.dequeue._2

              Pull.eval(head.sink.complete(ClientResponse.Executed(newS, out))) >>
                nextClientsAcc.dequeueOption.match
                  case Some((newHead, _)) => go(tail, newS, newHead, nextClientsAcc)
                  case None               => done(nextClientsAcc, newS)
            else
              go(tail, newS, head, clients)

      clients.dequeueOption match
        case None =>
          F.pure(clients, initS)

        case Some((head, _)) =>
          go(log.rangeStream(head.idx, commitIdx), initS, head, clients)
            .stream
            .compile
            .lastOrError
