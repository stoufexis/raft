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
  raftPort:        Port,
  clientPort:      Port
)

object KvStoreConfig:
  def loadFromEnv[F[_]](using env: Env[F], F: MonadThrow[F], log: Logger[F]): F[KvStoreConfig] =
    def getVar[A](name: String, f: String => Option[A]): F[A] =
      env
        .get(name)
        .flatMap(_.liftTo(RuntimeException(s"Missing env var: $name")))
        .flatMap(f(_).liftTo(RuntimeException(s"Could not parse $name")))

    def getCurrentNode: F[NodeId] =
      for
        internal <- getVar("CURRENT_NODE_INTERNAL", Some(_))
        external <- getVar("CURRENT_NODE_EXTERNAL", Some(_))
      yield NodeId(internal, external)

    def getOtherNodes: F[List[NodeId]] =
      for
        internal <- env.entries.map(_.collect { case (k, v) if k.startsWith("OTHER_NODE_INTERNAL") => v })
        external <- env.entries.map(_.collect { case (k, v) if k.startsWith("OTHER_NODE_EXTERNAL") => v })
      yield (internal.toList zip external.toList).map(NodeId(_, _))

    val cfg: F[KvStoreConfig] =
      for
        sqliteDbPath    <- getVar("SQLITE_DB_PATH", _.some)
        sqliteFetchSize <- getVar("SQLITE_FETCH_SIZE", _.toIntOption)
        raftPort        <- getVar("RAFT_PORT", _.toIntOption.flatMap(Port.fromInt))
        clientPort      <- getVar("CLIENT_PORT", _.toIntOption.flatMap(Port.fromInt))
        thisNode        <- getCurrentNode
        otherNodes      <- getOtherNodes
      yield KvStoreConfig(thisNode, otherNodes, sqliteDbPath, sqliteFetchSize, raftPort, clientPort)

    cfg.flatTap(x => log.debug(s"Loaded config as $x"))
