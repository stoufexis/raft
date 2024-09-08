package com.stoufexis.raft.persist

import cats.effect.kernel.*
import cats.implicits.given
import doobie.*
import doobie.implicits.given
import doobie.util.compat.FactoryCompat
import doobie.util.fragment.Fragment
import doobie.util.query.Query0
import doobie.util.transactor.Transactor
import fs2.*
import org.typelevel.log4cats.Logger

import com.stoufexis.raft.model.*
import com.stoufexis.raft.typeclass.Storeable

import scala.reflect.ClassTag

import java.sql.{Connection, DriverManager}

object SqlitePersistence:
  /** Uses a single connection, since this is never used concurrently.
    */
  def apply[F[_], A: ClassTag](dbPath: String, fetchSize: Int)(using
    F:         Async[F],
    logger:    Logger[F],
    storeable: Storeable[A]
  ): Resource[F, (Log[F, A], PersistedState[F])] =

    given Read[A]  = storeable.read
    given Write[A] = storeable.write

    case class LogRow(term: Term, entry: A)
    case class IndexlessLogRow(term: Term, entry: A)
    case class PersistedRow(term: Term, vote: Option[NodeId])

    val connectionResource: Resource[F, Connection] =
      Resource.fromAutoCloseable:
        F.blocking:
          Class.forName("org.sqlite.JDBC")
          DriverManager.getConnection(s"jdbc:sqlite:$dbPath")

    val columnsTypes: String =
      storeable
        .columns
        .map((name, typ) => s"$name $typ")
        .mkString(",\n")

    val columns: String =
      storeable.columns.map(_._1).mkString(" ,")

    val columnsCnt: Int =
      storeable.columns.size

    val createLogTable: Fragment =
      sql"CREATE TABLE IF NOT EXISTS log(term INTEGER NOT NULL, " ++ Fragment.const(columnsTypes) ++ sql");"

    val createPersistTable: Fragment =
      sql"CREATE TABLE IF NOT EXISTS persisted_state(term INTEGER PRIMARY KEY, vote TEXT);"

    def insertManyIndexless(ps: Chunk[IndexlessLogRow]): ConnectionIO[Int] =
      val sql = "insert into log (term, " + columns + ") values (? " + (",?" * columnsCnt) + ")"
      Update[IndexlessLogRow](sql).updateMany(ps)

    for
      xa: Transactor[F] <-
        connectionResource.map(Transactor.fromConnection(_, None))

      _ <- Resource.eval:
        logger.info(s"Setting up log table: ${createLogTable.internals.sql}")
          >> createLogTable.update.run.transact(xa)
          >> logger.info(s"Setting up log table: ${createPersistTable.internals.sql}")
          >> createPersistTable.update.run.transact(xa)

      persistentState: PersistedState[F] = new:
        def persist(term: Term, vote: Option[NodeId]): F[Unit] =
          sql"""
          INSERT INTO persisted_state(term,vote) VALUES (${PersistedRow(term, vote)}) 
          ON CONFLICT (term) DO UPDATE SET vote = excluded.vote;
          """.update.run.transact(xa).void

        def readLatest: F[(Term, Option[NodeId])] =
          sql"SELECT term, vote FROM persisted_state ORDER BY term DESC LIMIT 1"
            .query[(Term, Option[NodeId])]
            .unique
            .transact(xa)

      log: Log[F, A] = new:
        def rangeQuery(from: Index, until: Index): Query0[(Index, LogRow)] =
          sql"SELECT rowid, * FROM log WHERE rowid >= $from AND rowid <= $until ORDER BY rowid ASC"
            .query[(Index, LogRow)]

        def termCIO(index: Index): ConnectionIO[Term] =
          sql"SELECT term FROM log WHERE rowid = $index"
            .query[Term].unique

        def deleteAfter(index: Index): ConnectionIO[Unit] =
          sql"DELETE FROM log WHERE rowid > $index"
            .update.run.void

        def append(term: Term, entries: Chunk[A]): ConnectionIO[Unit] =
          insertManyIndexless(entries.map(IndexlessLogRow(term, _))).void

        def matches(term: Term, index: Index): ConnectionIO[Boolean] =
          sql"SELECT rowid FROM log WHERE rowid = $index AND term = $term"
            .query[Index].option.map(_.isDefined)

        def lastTermIndexCIO: ConnectionIO[(Term, Index)] =
          sql"SELECT term, rowid FROM log ORDER BY rowid DESC LIMIT 1"
            .query[(Term, Index)]
            .unique

        def appendChunkCIO(term: Term, prevrowid: Index, entries: Chunk[A]): ConnectionIO[Index] =
          for
            _      <- deleteAfter(prevrowid)
            _      <- append(term, entries)
            (_, i) <- lastTermIndexCIO
          yield i

        def term(index: Index): F[Term] =
          termCIO(index).transact(xa)

        def range(from: Index, until: Index): F[Chunk[A]] =
          rangeQuery(from, until)
            .map(_._2.entry)
            .to[Array]
            .map(Chunk.array)
            .transact(xa)

        def rangeStream(from: Index, until: Index): Stream[F, (Index, A)] =
          rangeQuery(from, until)
            .map((idx, x) => (idx, x.entry))
            .streamWithChunkSize(fetchSize)
            .transact(xa)

        def overwriteChunkIfMatches(
          prevLogTerm:  Term,
          prevLogIndex: Index,
          term:         Term,
          entries:      Chunk[A]
        ): F[Option[Index]] =
          matches(prevLogTerm, prevLogIndex).ifM(
            appendChunkCIO(term, prevLogIndex, entries).map(Some(_)),
            Option.empty[Index].pure[ConnectionIO]
          ).transact(xa)

        def appendChunk(term: Term, prevLogIndex: Index, entries: Chunk[A]): F[Index] =
          lastTermIndexCIO.map(_._2 == prevLogIndex).ifM(
            appendChunkCIO(term, prevLogIndex, entries),
            FC.raiseError(IllegalStateException("appendChunk not called at log end"))
          ).transact(xa)

        def lastTermIndex: F[(Term, Index)] =
          lastTermIndexCIO.transact(xa)
    yield (log, persistentState)
