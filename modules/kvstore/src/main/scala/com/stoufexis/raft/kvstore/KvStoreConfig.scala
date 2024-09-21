package com.stoufexis.raft.kvstore

import cats.MonadThrow
import cats.effect.std.Env
import cats.implicits.given
import com.comcast.ip4s.Port
import org.typelevel.log4cats.Logger

import com.stoufexis.raft.model.NodeId

case class KvStoreConfig(
  thisNode:        NodeId,
  otherNodes:      List[NodeId],
  sqliteDbPath:    String,
  sqliteFetchSize: Int,
  httpPort:        Port,
)

object KvStoreConfig:
  def loadFromEnv[F[_]](using env: Env[F], F: MonadThrow[F], log: Logger[F]): F[KvStoreConfig] =
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

    val cfg: F[KvStoreConfig] =
      for
        sqliteDbPath    <- getVar("SQLITE_DB_PATH", _.some)
        sqliteFetchSize <- getVar("SQLITE_FETCH_SIZE", _.toIntOption)
        httpPort        <- getVar("HTTP_PORT", _.toIntOption.flatMap(Port.fromInt))
        thisNode        <- getVar("CURRENT_NODE", x => Some(NodeId(s"$x:${httpPort.value}")))
        otherNodes      <- getVars("OTHER_NODES", x => Some(NodeId(s"$x:${httpPort.value}")))
      yield KvStoreConfig(thisNode, otherNodes, sqliteDbPath, sqliteFetchSize, httpPort)

    cfg.flatTap(x => log.debug(s"Loaded config as $x"))
