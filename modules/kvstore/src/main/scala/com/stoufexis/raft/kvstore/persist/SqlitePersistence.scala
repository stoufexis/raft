package com.stoufexis.raft.kvstore.persist

import cats.MonadThrow
import cats.data.NonEmptySeq
import cats.effect.kernel.*
import cats.implicits.given
import doobie.*
import doobie.implicits.given
import doobie.util.*
import doobie.util.fragment.Fragment
import doobie.util.transactor.Transactor
import fs2.*
import org.typelevel.log4cats.Logger
import scodec.Attempt.*
import scodec.Codec
import scodec.bits.BitVector

import com.stoufexis.raft.model.*
import com.stoufexis.raft.persist.*
import com.stoufexis.raft.kvstore.implicits.given

import java.sql.{Connection, DriverManager}

object SqlitePersistence:
  /** Uses a single connection since there is no concurrent use.
    */
  def apply[F[_], A: Codec](dbPath: String, fetchSize: Int)(using
    F:      Async[F],
    logger: Logger[F]
  ): Resource[F, (Log[F, A], PersistedState[F])] =

    case object EncodingFailure extends RuntimeException("Encoding failure")

    case class LogRow(term: Term, cid: CommandId, entry: Array[Byte]):
      def getCommand[G[_]](using G: MonadThrow[G]): G[Command[A]] =
        Codec[A].decode(BitVector(entry)) match
          case Successful(a)  => G.pure(Command(cid, a.value))
          case Failure(cause) => G.raiseError(IllegalStateException(s"Corrupted log: $cause"))

    object LogRow:
      def fromCommand[G[_]](term: Term, command: Command[A])(using G: MonadThrow[G]): G[LogRow] =
        Codec[A].encode(command.value) match
          case Successful(value) => G.pure(LogRow(term, command.id, value.toByteArray))
          case Failure(cause)    => G.raiseError(RuntimeException(s"Encoding failure: $cause"))

    case class PersistedRow(term: Term, vote: Option[NodeId])

    val connectionResource: Resource[F, Connection] =
      Resource.fromAutoCloseable:
        F.blocking:
          Class.forName("org.sqlite.JDBC")
          DriverManager.getConnection(s"jdbc:sqlite:$dbPath")

    val createLogTable: Fragment =
      sql"""
        CREATE TABLE IF NOT EXISTS log(
            term INTEGER NOT NULL,
            cid TEXT NOT NULL,
            entry BLOB NOT NULL
        );
        CREATE UNIQUE INDEX IF NOT EXISTS log_unique_cid ON log(cid);
      """

    val createPersistTable: Fragment =
      sql"""
        CREATE TABLE IF NOT EXISTS persisted_state(
            term INTEGER PRIMARY KEY, 
            vote TEXT
        );
      """

    for
      xa: Transactor[F] <-
        connectionResource.map(Transactor.fromConnection(_, None))

      _ <- Resource.eval:
        logger.info(s"Setting up log table: ${createLogTable.internals.sql}")
          >> createLogTable.update.run.transact(xa)
          >> logger.info(s"Setting up persist table: ${createPersistTable.internals.sql}")
          >> createPersistTable.update.run.transact(xa)

      persistentState: PersistedState[F] = new:
        def persist(term: Term, vote: Option[NodeId]): F[Unit] =
          sql"""
          INSERT INTO persisted_state(term,vote) VALUES (${PersistedRow(term, vote)}) 
          ON CONFLICT (term) DO UPDATE SET vote = excluded.vote;
          """.update.run.transact(xa).void

        def readLatest: F[Option[(Term, Option[NodeId])]] =
          sql"SELECT term, vote FROM persisted_state ORDER BY term DESC LIMIT 1"
            .query[(Term, Option[NodeId])]
            .option
            .transact(xa)

      log: Log[F, A] = new:
        def append(term: Term, entries: NonEmptySeq[Command[A]]): F[Index] =
          val insertSql: String =
            "INSERT INTO log (term, cid, entry) VALUES (?, ?, ?)"

          val maxRowId: ConnectionIO[Index] =
            sql"SELECT max(rowid) FROM log".query[Index].unique

          val rows: ConnectionIO[NonEmptySeq[LogRow]] =
            entries.traverse(LogRow.fromCommand(term, _))

          rows.flatMap(Update[LogRow](insertSql).updateMany(_))
            .flatMap(_ => maxRowId)
            .transact(xa)

        def commandIdExists(commandId: CommandId): F[Boolean] =
          sql"SELECT true FROM log WHERE cid = $commandId"
            .query[Boolean].option.map(_.getOrElse(false)).transact(xa)

        def deleteAfter(index: Index): F[Unit] =
          sql"DELETE FROM log WHERE rowid > $index"
            .update.run.transact(xa).void

        def term(index: Index): F[Term] =
          sql"SELECT term FROM log WHERE rowid = $index"
            .query[Term].unique.transact(xa)

        def rangeStream(from: Index, until: Index): Stream[F, (Index, Command[A])] =
          sql"SELECT rowid, * FROM log WHERE rowid >= $from AND rowid <= $until"
            .query[(Index, LogRow)]
            .stream
            .transact(xa)
            .evalMap((i, row) => row.getCommand.map((i, _)))

        def lastTermIndex: F[Option[(Term, Index)]] =
          sql"""
            WITH max_rowid AS (
              SELECT max(rowid) AS mrowid FROM log
            )
            SELECT term, rowid 
            FROM log
            JOIN max_rowid ON (log.rowid = max_rowid.mrowid)
          """
            .query[(Term, Index)]
            .option
            .transact(xa)

        def matches(prevLogTerm: Term, prevLogIndex: Index): F[Boolean] =
          sql"SELECT true FROM log WHERE term = $prevLogTerm AND rowid = $prevLogIndex"
            .query[Boolean]
            .option
            .map(_.getOrElse(false))
            .transact(xa)

        def range(from: Index, until: Index): F[Seq[Command[A]]] =
          sql"SELECT * FROM log WHERE rowid >= $from AND rowid <= $until"
            .query[LogRow]
            .stream
            .evalMap(_.getCommand[ConnectionIO])
            .compile
            .toList
            .widen
            .transact(xa)
    yield (log, persistentState)
