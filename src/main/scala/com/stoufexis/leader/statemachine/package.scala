package com.stoufexis.leader.statemachine

import cats.*
import cats.effect.kernel.*
import cats.effect.std.Queue
import cats.effect.std.QueueSink
import cats.effect.std.QueueSource
import cats.implicits.given
import fs2.*
import fs2.concurrent.SignallingRef
import org.typelevel.log4cats.Logger

import com.stoufexis.leader.model.*
import com.stoufexis.leader.rpc.*
import com.stoufexis.leader.service.*
import com.stoufexis.leader.typeclass.Counter.*
import com.stoufexis.leader.util.*

import scala.concurrent.duration.FiniteDuration

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
      _ <- logger.info(s"Append accepted from ${req.leaderId}")
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