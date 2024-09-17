package com.stoufexis.raft.kvstore

import cats.MonadThrow
import cats.effect.std.Env
import cats.implicits.given

import com.stoufexis.raft.model.NodeId

case class KvStoreConfig(
  thisNode:        NodeId,
  otherNodes:      List[NodeId],
  sqliteDbPath:    String,
  sqliteFetchSize: Int
)

object KvStoreConfig:
  def loadFromEnv[F[_]](using env: Env[F], F: MonadThrow[F]): F[KvStoreConfig] =
    def getVar[A](name: String, f: String => Option[A]): F[A] =
      env
        .get(name)
        .flatMap(_.liftTo(RuntimeException(s"Missing env var: $name")))
        .flatMap(f(_).liftTo(RuntimeException(s"Could not parse $name")))

    def getVars[A](name: String, f: String => Option[A]): F[List[A]] =
      getVar(name, Some(_)).flatMap: x =>
        x.split(",")
          .toList
          .traverse(f(_).liftTo(RuntimeException(s"Could not parse $name")))

    for
      thisNode        <- getVar("CURRENT_NODE", _.some.map(NodeId(_)))
      otherNodes      <- getVars("OTHER_NODES", _.some.map(NodeId(_)))
      sqliteDbPath    <- getVar("SQLITE_DB_PATH", _.some)
      sqliteFetchSize <- getVar("SQLITE_FETCH_SIZE", _.toIntOption)
    yield KvStoreConfig(thisNode, otherNodes, sqliteDbPath, sqliteFetchSize)
