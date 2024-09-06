package com.stoufexis.raft.statemachine

import cats.*
import cats.effect.kernel.*
import cats.effect.std.Queue
import cats.implicits.given
import fs2.*
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

enum ResettableTimeout[A] derives CanEqual:
  case Reset[A]()        extends ResettableTimeout[A]
  case Skip[A]()         extends ResettableTimeout[A]
  case Output[A](out: A) extends ResettableTimeout[A]

  def fold[B](onReset: => B, onSkip: => B, onOutput: A => B): B =
    this match
      case Reset()     => onReset
      case Skip()      => onSkip
      case Output(out) => onOutput(out)

extension [F[_], A](stream: Stream[F, A])

  def increasing(using IntLike[A]): Stream[F, A] =
    stream.filterWithPrevious(_ < _)

  /** Backpressures by dropping old elements from upstream
    *
    * TODO: test
    */
  def dropping(buffer: Int)(using F: Concurrent[F]): Stream[F, A] =
    Stream
      .eval(Queue.dropping[F, A](buffer) product Deferred[F, Unit])
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
    stream
      .map(Option(_))
      .timeoutOnPullTo(repeatEvery, Stream(Option.empty[A]))
      .mapFilterAccumulate(Option.empty[A]):
        case (last, None) => (last, last)
        case (last, next) => (next, next)

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
  )(f: (S, A) => (S, F[Unit], ResettableTimeout[B]))(using Temporal[F]): Stream[F, B] =
    def go(leftover: Chunk[A], timed: Pull.Timed[F, A], s: S): Pull[F, B, Unit] =
      if leftover.nonEmpty then
        f(s, leftover.head.get) match
          case (_, fu, ResettableTimeout.Output(out)) =>
            Pull.output1(out) >> Pull.eval(fu) >> Pull.done

          case (s2, fu, ResettableTimeout.Reset()) =>
            timed.timeout(timeout) >> Pull.eval(fu) >> go(leftover.drop(1), timed, s2)

          case (s2, fu, ResettableTimeout.Skip()) =>
            Pull.eval(fu) >> go(leftover.drop(1), timed, s2)
      else
        timed.uncons.flatMap:
          case None =>
            Pull.done

          case Some((Right(chunk), next)) =>
            go(chunk, next, s)

          case Some((Left(_), next)) =>
            Pull.eval(onTimeout).flatMap(Pull.output1(_)) >> Pull.done
    end go

    stream.pull
      .timed(t => t.timeout(timeout) >> go(Chunk.empty, t, init))
      .stream

  def resettableTimeout[B](
    timeout:   FiniteDuration,
    onTimeout: F[B]
  )(f: A => (F[Unit], ResettableTimeout[B]))(using Temporal[F]): Stream[F, B] =
    resettableTimeoutAccumulate((), timeout, onTimeout)((_, a) => () *: f(a))

  def evalMapFirstSome[B](f: A => F[Option[B]]): Stream[F, B] =
    stream.evalMapFilter(f).head

  def evalMapFilterAccumulate[S, B](init: S)(f: (S, A) => F[(S, Option[B])]): Stream[F, B] =
    stream.evalMapAccumulate(init)(f).mapFilter(_._2)

  def mapFilterAccumulate[S, B](init: S)(f: (S, A) => (S, Option[B])): Stream[F, B] =
    stream.mapAccumulate(init)(f).mapFilter(_._2)

def raceFirst[F[_]: Concurrent, A](streams: List[Stream[F, A]]): Stream[F, A] =
  streams.map(_.head)
    .parJoinUnbounded
    .head

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
  def termExpired(state: NodeInfo[?], sink: DeferredSink[F, AppendResponse]): F[Unit] =
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
      _ <- logger.debug(s"Append accepted from ${req.leaderId}")
    yield ()

  def inconsistent(sink: DeferredSink[F, AppendResponse]): F[Unit] =
    for
      _ <- sink.complete_(AppendResponse.NotConsistent)
      _ <- logger.warn(s"Append from ${req.leaderId} was inconsistent with local log")
    yield ()

extension [F[_]](req: RequestVote)(using F: MonadThrow[F], logger: Logger[F])
  def termExpired(state: NodeInfo[?], sink: DeferredSink[F, VoteResponse]): F[Unit] =
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