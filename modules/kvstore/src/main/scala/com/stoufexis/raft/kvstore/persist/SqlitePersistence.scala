package com.stoufexis.raft.kvstore.persist

import _root_.io.circe.*
import _root_.io.circe.syntax.*
import cats.MonadThrow
import cats.data.NonEmptySeq
import cats.effect.kernel.*
import cats.implicits.given
import com.zaxxer.hikari.HikariConfig
import doobie.*
import doobie.hikari.HikariTransactor
import doobie.implicits.given
import doobie.util.*
import doobie.util.fragment.Fragment
import fs2.*
import org.typelevel.log4cats.Logger

import com.stoufexis.raft.kvstore.implicits.given
import com.stoufexis.raft.model.*
import com.stoufexis.raft.persist.*

object SqlitePersistence:
  def apply[F[_], A: Codec](dbPath: String, poolSize: Int)(using
    F:      Async[F],
    logger: Logger[F]
  ): Resource[F, (Log[F, A], PersistedState[F])] =

    case object EncodingFailure extends RuntimeException("Encoding failure")

    case class LogRow(term: Term, cid: CommandId, entry: Json):
      def getCommand[G[_]](using G: MonadThrow[G]): G[Command[A]] =
        Codec[A].decodeJson(entry) match
          case Left(err) => G.raiseError(err)
          case Right(a)  => G.pure(Command(cid, a))

    object LogRow:
      def fromCommand(term: Term, command: Command[A]): LogRow =
        LogRow(term, command.id, command.value.asJson)

    case class PersistedRow(term: Term, vote: Option[NodeId])

    val createLogTable: Fragment =
      sql"""
        CREATE TABLE IF NOT EXISTS log(
            term  INTEGER NOT NULL,
            cid   TEXT NOT NULL,
            entry TEXT NOT NULL
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

    val hikariCfg: HikariConfig =
      // For the full list of hikari configurations see https://github.com/brettwooldridge/HikariCP#gear-configuration-knobs-baby
      val config = new HikariConfig()
      config.setDriverClassName("org.sqlite.JDBC")
      config.setJdbcUrl(s"jdbc:sqlite:$dbPath")
      config.setMaximumPoolSize(poolSize)
      config

    /**
     * For anyone looking into this not familiar with doobie; keep in mind that even though some queries look like
     * they are using direct sql interpolation, that is not the case. Doobie translates strings using the sql interpolation
     * syntax to jdbc PreparedStatements and sets all parameters using the proper `set` methods.
     *
     * For further reading, see https://typelevel.org/doobie/docs/05-Parameterized.html
     */
    for
      xa: HikariTransactor[F] <-
        HikariTransactor.fromHikariConfig(hikariCfg)

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

          val rows: NonEmptySeq[LogRow] =
            entries.map(LogRow.fromCommand(term, _))

          Update[LogRow](insertSql)
            .updateMany(rows)
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
