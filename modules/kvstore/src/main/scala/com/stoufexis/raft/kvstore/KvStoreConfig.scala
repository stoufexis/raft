package com.stoufexis.raft.kvstore

import cats.MonadThrow
import cats.effect.std.Env
import cats.implicits.given
import com.comcast.ip4s.Port
import org.typelevel.log4cats.Logger

import com.stoufexis.raft.model.NodeId

import scala.concurrent.duration.*
import scala.concurrent.duration.FiniteDuration

case class KvStoreConfig(
  thisNode:            NodeId,
  otherNodes:          List[NodeId],
  sqliteDbPath:        String,
  httpPort:            Port,
  electionTimeoutLow:  FiniteDuration,
  electionTimeoutHigh: FiniteDuration,
  heartbeatEvery:      FiniteDuration,
  clientRetryAfter:    FiniteDuration
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
        sqliteDbPath    <- getVar("KV_SQLITE_DB_PATH", _.some)
        httpPort        <- getVar("KV_HTTP_PORT", _.toIntOption.flatMap(Port.fromInt))
        electionLow     <- getVar("KV_ELECTION_TIMEOUT_LOW_MS", _.toLongOption.map(_.millis))
        electionHigh    <- getVar("KV_ELECTION_TIMEOUT_HIGH_MS", _.toLongOption.map(_.millis))
        heartbeatEvery  <- getVar("KV_HEARTBEAT_EVERY_MS", _.toLongOption.map(_.millis))
        retryAfter      <- getVar("KV_CLIENT_RETRY_AFTER_MS", _.toLongOption.map(_.millis))
        thisNode        <- getVar("KV_CURRENT_NODE", x => Some(NodeId(s"$x:${httpPort.value}")))
        otherNodes      <- getVars("KV_OTHER_NODES", x => Some(NodeId(s"$x:${httpPort.value}")))
      yield KvStoreConfig(
        thisNode            = thisNode,
        otherNodes          = otherNodes,
        sqliteDbPath        = sqliteDbPath,
        httpPort            = httpPort,
        electionTimeoutLow  = electionLow,
        electionTimeoutHigh = electionHigh,
        heartbeatEvery      = heartbeatEvery,
        clientRetryAfter    = retryAfter
      )

    cfg.flatTap(x => log.debug(s"Loaded config as $x"))
