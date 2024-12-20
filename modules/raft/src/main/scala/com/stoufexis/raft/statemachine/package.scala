package com.stoufexis.raft.statemachine

import cats.*
import cats.effect.kernel.*
import cats.effect.std.Queue
import cats.implicits.given
import fs2.*
import fs2.concurrent.Channel
import org.typelevel.log4cats.Logger

import com.stoufexis.raft.model.*
import com.stoufexis.raft.rpc.*
import com.stoufexis.raft.typeclass.IntLike
import com.stoufexis.raft.typeclass.IntLike.*

import scala.concurrent.duration.FiniteDuration

extension [F[_]: Monad, A](deferred: DeferredSink[F, A])
  def complete_(a: A): F[Unit] = deferred.complete(a).void

extension [F[_]: Monad](deferred: DeferredSink[F, Unit])
  def complete_ : F[Unit] = deferred.complete(()).void

enum ResettableTimeout[F[_], A, S] derives CanEqual:
  case Reset[F[_], A, S](state: F[S]) extends ResettableTimeout[F, A, S]
  case Skip[F[_], A, S](state: F[S])  extends ResettableTimeout[F, A, S]
  case Output[F[_], A, S](out: F[A])  extends ResettableTimeout[F, A, S]

object ResettableTimeout:
  def outputPure[F[_]: Applicative, A, S](a: A): ResettableTimeout[F, A, S] =
    ResettableTimeout.Output(a.pure)

extension [F[_], A](stream: Stream[F, A])

  def increasing(using IntLike[A]): Stream[F, A] =
    stream.filterWithPrevious(_ < _)

  /** Backpressures by dropping old elements from upstream
    *
    * TODO: test
    */
  def dropping(buffer: Int)(using F: Concurrent[F]): Stream[F, A] =
    Stream
      .eval(Queue.circularBuffer[F, A](buffer) product Deferred[F, Unit])
      .flatMap: (q, d) =>
        val updater: Stream[F, Nothing] =
          stream
            .evalMap(q.offer)
            .onFinalize(d.complete_)
            .drain

        Stream
          .fromQueueUnterminated(q)
          .concurrently(updater)
          .interruptWhen(d.get.attempt)

  /** If there is no new element within repeatEvery, repeat the previously emitted element.
    *
    * TODO: test
    */
  def repeatLast(repeatEvery: FiniteDuration)(using F: Temporal[F]): Stream[F, A] =
    def go(tp: Pull.Timed[F, A], previous: Option[A]): Pull[F, A, Unit] =
      tp.timeout(repeatEvery) >> tp.uncons.flatMap:
        case Some((Right(elems), next)) => Pull.output(elems) >> go(next, elems.last)
        case Some((Left(_), next))      => Pull.outputOption1(previous) >> go(next, previous)
        case None                       => Pull.done

    stream.pull.timed(go(_, None)).stream

  def evalScanDrain[S](init: S)(f: (S, A) => F[S]): Stream[F, Nothing] =
    stream.evalScan(init)(f).drain

  def mergeEither[B](that: Stream[F, B])(using F: Concurrent[F]): Stream[F, Either[A, B]] =
    stream.map(Left(_)).mergeHaltBoth(that.map(Right(_)))

  // TODO: Test
  // Terminates after first output
  def resettableTimeoutAccumulate[S, B](
    init:      S,
    timeout:   FiniteDuration,
    onTimeout: F[B]
  )(f: (S, A) => ResettableTimeout[F, B, S])(using F: Temporal[F]): Stream[F, B] =
    def timer(d: Deferred[F, Option[B]], ch: Channel[F, Unit]): Stream[F, Unit] =
      ch.stream
        .switchMap(_ => Stream.eval(F.sleep(timeout)))
        .evalMap(_ => d.complete_(None))

    def go(
      s:   Stream[F, A],
      d:   Deferred[F, Option[B]],
      ch:  Channel[F, Unit],
      acc: S
    ): Pull[F, Nothing, Unit] =
      s.pull.uncons1.flatMap:
        case None =>
          Pull.done

        case Some((a, tail)) =>
          f(acc, a) match
            case ResettableTimeout.Reset(state) =>
              Pull.eval(ch.send(())) >> Pull.eval(state).flatMap(go(tail, d, ch, _))

            case ResettableTimeout.Skip(state) =>
              Pull.eval(state).flatMap(go(tail, d, ch, _))

            case ResettableTimeout.Output(out) =>
              Pull.eval(out).flatMap(o => Pull.eval(d.complete_(Some(o))))

    for
      kill  <- Stream.eval(Deferred[F, Option[B]])
      reset <- Stream.eval(Channel.synchronous[F, Unit])
      _     <- timer(kill, reset).interruptWhen(kill.get.void.attempt).spawn
      _     <- go(stream, kill, reset, init).stream.interruptWhen(kill.get.void.attempt).spawn
      _     <- Stream.eval(reset.send(()))
      out   <- Stream.eval(kill.get.flatMap(_.fold(onTimeout)(F.pure)))
    yield out

  def evalMapFirstSome[B](f: A => F[Option[B]]): Stream[F, B] =
    stream.evalMapFilter(f).head

  def evalMapAccumulateFirstSome[S, B](init: S)(f: (S, A) => F[(S, Option[B])]): Stream[F, B] =
    stream.evalMapFilterAccumulate(init)(f).head

  def evalMapFilterAccumulate[S, B](init: S)(f: (S, A) => F[(S, Option[B])]): Stream[F, B] =
    stream.evalMapAccumulate(init)(f).mapFilter(_._2)

  def mapFilterAccumulate[S, B](init: S)(f: (S, A) => (S, Option[B])): Stream[F, B] =
    stream.mapAccumulate(init)(f).mapFilter(_._2)

def raceFirst[F[_]: Concurrent, A](streams: List[Stream[F, A]]): Stream[F, A] =
  streams.map(_.head)
    .parJoinUnbounded
    .head

def raceFirst[F[_]: Concurrent, A](head: Stream[F, A], tail: Stream[F, A]*): Stream[F, A] =
  raceFirst(head :: List(tail*))

def raceFirstOrError[F[_]: Concurrent, A](streams: List[Stream[F, A]]): F[A] =
  raceFirst(streams)
    .compile
    .lastOrError

def repeatOnInterval[F[_]: Temporal, A](
  delay: FiniteDuration,
  fa:    F[A]
): Stream[F, A] =
  (Stream.unit ++ Stream.fixedDelay(delay)) >> Stream.eval(fa)

def parRepeat[F[_]: Temporal, A, B](
  tos:         Iterable[B],
  repeatEvery: FiniteDuration
)(f: B => F[A]): Stream[F, (B, A)] =
  Stream
    .iterable(tos)
    .map(to => repeatOnInterval(repeatEvery, f(to)).map((to, _)))
    .parJoinUnbounded

extension [F[_]](req: AppendEntries[?])(using F: MonadThrow[F], logger: Logger[F])
  def termExpired(state: NodeInfo, sink: DeferredSink[F, AppendResponse]): F[Unit] =
    for
      _ <- sink.complete_(AppendResponse.TermExpired(state.term))
      _ <- logger.warn(s"Detected stale leader ${req.leaderId}")
    yield ()

  def duplicateLeaders[A](sink: DeferredSink[F, AppendResponse]): F[A] =
    for
      msg <- F.pure("Duplicate leaders for term")
      _   <- sink.complete_(AppendResponse.IllegalState(msg))
      n: Nothing <- F.raiseError(IllegalStateException(msg))
    yield n

  def accepted(sink: DeferredSink[F, AppendResponse]): F[Unit] =
    for
      _ <- sink.complete_(AppendResponse.Accepted)
      _ <- F.pure(req.entries.isEmpty).ifM(
        logger.debug(s"Heartbeat accepted from ${req.leaderId}"),
        logger.info(s"Append accepted from ${req.leaderId}")
      )
    yield ()

  def inconsistent(sink: DeferredSink[F, AppendResponse]): F[Unit] =
    for
      _ <- sink.complete_(AppendResponse.NotConsistent)
      _ <- logger.warn(s"Append from ${req.leaderId} was inconsistent with local log")
    yield ()

extension [F[_]](req: RequestVote)(using F: MonadThrow[F], logger: Logger[F])
  def termExpired(state: NodeInfo, sink: DeferredSink[F, VoteResponse]): F[Unit] =
    for
      _ <- sink.complete_(VoteResponse.TermExpired(state.term))
      _ <- logger.warn(s"Detected stale candidate ${req.candidateId}")
    yield ()

  def reject(sink: DeferredSink[F, VoteResponse]): F[Unit] =
    for
      _ <- sink.complete_(VoteResponse.Rejected)
      _ <- logger.info(s"Rejected vote for ${req.candidateId}. Its term was ${req.term}")
    yield ()

  def grant(sink: DeferredSink[F, VoteResponse]): F[Unit] =
    for
      _ <- sink.complete_(VoteResponse.Granted)
      _ <- logger.info(s"Voted for ${req.candidateId} in term ${req.term}")
    yield ()

extension [F[_], A, S](reqs: Stream[F, IncomingClientRequest[F, A, ?, S]])
  def respondWithLeader(leader: Option[NodeId]): Stream[F, Nothing] =
    reqs
      .evalMap(_.sink.complete(ClientResponse.knownLeader(leader)))
      .drain
