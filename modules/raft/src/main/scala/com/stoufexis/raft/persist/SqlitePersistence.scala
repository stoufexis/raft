package com.stoufexis.raft.persist

import cats.effect.kernel.*
import cats.implicits.given
import doobie.*
import doobie.given
import doobie.implicits.given
import doobie.util.compat.FactoryCompat
import doobie.util.fragment.Fragment
import doobie.util.log.LogEvent
import doobie.util.transactor.Transactor
import fs2.*
import org.typelevel.log4cats.Logger

import com.stoufexis.raft.model.*
import com.stoufexis.raft.typeclass.Storeable
import com.stoufexis.raft.typeclass.Storeable.given

import scala.collection.mutable
import scala.reflect.ClassTag

import java.sql.{Connection, DriverManager}

object SqlitePersistence:
  /** Uses a single connection, since this is never used concurrently.
    */
  def apply[F[_], A: ClassTag, S](dbPath: String)(using
    F:         Async[F],
    logger:    Logger[F],
    storeable: Storeable[A]
  ): F[(Log[F, A], PersistedState[F])] =

    case class LogRow(index: Index, term: Term, entry: A)
    case class IndexlessLogRow(term: Term, entry: A)
    case class PersistedRow(term: Term, vote: NodeId)

    val connectionResource: Resource[F, Connection] =
      Resource.fromAutoCloseable:
        F.blocking:
          Class.forName("org.sqlite.JDBC");
          DriverManager.getConnection(s"jdbc:sqlite:$dbPath")

    val columnsTypes: String =
      storeable
        .columns
        .map((name, typ) => s"$name ${typ.typeName}")
        .mkString(",\n")

    val columns: String =
      storeable.columns.map(_._1).mkString(",")

    val ddl: Fragment =
      sql"""
        CREATE TABLE IF NOT EXISTS log(
          index INTEGER PRIMARY KEY,
          term INTEGER NOT NULL,
      """ ++ Fragment.const(columnsTypes) ++ sql"""
        );

        CREATE TABLE IF NOT EXISTS persisted_state(
          term INTEGER PRIMARY KEY,
          vote TEXT
        );
      """

    def insertManyIndexless(ps: Chunk[IndexlessLogRow]): ConnectionIO[Int] =
      val sql = "insert into person (term, " + columns + ") values (?, ?)"
      Update[IndexlessLogRow](sql).updateMany(ps)

    for
      xa: Transactor[F] <-
        connectionResource.map(Transactor.fromConnection(_, None))

      _ <- Resource.eval:
        logger.info(s"Setting up ddl: ${ddl.internals.sql}")
          >> ddl.update.run.transact(xa)

      log: Log[F, A] = new:
        def rangeCIO(from: Index, until: Index): ConnectionIO[Chunk[A]] =
          sql"SELECT * FROM log WHERE index >= $from AND index <= $until ORDER BY index ASC"
            .query[LogRow].map(_.entry).to[Array].map(Chunk.array)

        def termCIO(index: Index): ConnectionIO[Term] =
          sql"SELECT term FROM log WHERE index = $index"
            .query[Term].unique

        def deleteAfter(index: Index): ConnectionIO[Unit] =
          sql"DELETE FROM log WHERE index > $index"
            .update.run.void

        def append(term: Term, entries: Chunk[A]): ConnectionIO[Unit] =
          insertManyIndexless(entries.map(IndexlessLogRow(term, _))).void

        def matches(term: Term, index: Index): ConnectionIO[Boolean] =
          sql"SELECT index FROM log WHERE index = $index AND term = $term"
            .query.option.map(_.isDefined)

        def lastTermIndexCIO: ConnectionIO[(Term, Index)] =
          sql"SELECT term, index FROM log ORDER BY index DESC LIMIT 1;"
            .query[(Term, Index)]
            .unique

        def appendChunkCIO(term: Term, prevIdx: Index, entries: Chunk[A]): ConnectionIO[Index] =
          for
            _      <- deleteAfter(prevIdx)
            _      <- append(term, entries)
            (_, i) <- lastTermIndexCIO
          yield i

        def term(index: Index): F[Term] =
          termCIO(index).transact(xa)

        def range(from: Index, until: Index): F[(Term, Chunk[A])] =
          termCIO(from)
            .flatMap(t => rangeCIO(from, until).tupleLeft(t))
            .transact(xa)

        def readUntil(until: Index): Stream[F, (Index, A)] =
          sql"SELECT * FROM log WHERE index <= $until ORDER BY index ASC"
            .query[LogRow]
            .map(x => (x.index, x.entry))
            .stream
            .transact(xa)

        def appendChunkIfMatches(
          prevLogTerm:  Term,
          prevLogIndex: Index,
          term:         Term,
          entries:      Chunk[A]
        ): F[Option[Index]] =
          matches(prevLogTerm, prevLogIndex).ifM(
            appendChunkCIO(term, prevLogIndex, entries).map(Some(_)),
            Option.empty[Index].pure[ConnectionIO]
          ).transact(xa)

        def appendChunk(term: Term, prevIdx: Index, entries: Chunk[A]): F[Index] =
          appendChunkCIO(term, prevIdx, entries).transact(xa)

        def lastTermIndex: F[(Term, Index)] =
          lastTermIndexCIO.transact(xa)
    yield ()

    ???
