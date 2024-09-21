package com.stoufexis.raft.kvstore

import cats.effect.*
import cats.implicits.given
import com.comcast.ip4s.*
import org.http4s.ember.client.EmberClientBuilder
import org.http4s.ember.server.EmberServerBuilder
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import com.stoufexis.raft.RaftNode
import com.stoufexis.raft.kvstore.persist.SqlitePersistence
import com.stoufexis.raft.kvstore.rpc.Routes
import com.stoufexis.raft.kvstore.rpc.RpcClient
import com.stoufexis.raft.kvstore.statemachine.*

import scala.concurrent.duration.*

object Main extends IOApp.Simple:
  def raftNode(cfg: KvStoreConfig)(using
    Logger[IO]
  ): Resource[IO, RaftNode[IO, KvCommand, KvResponse, KvState]] =
    for
      (log, persist) <-
        SqlitePersistence[IO, KvCommand](
          dbPath = cfg.sqliteDbPath,
          // The leader uses the most connecitons.
          // At a maximum it can cocurrently use a connection for each node it appends to,
          // plus one connection for appending entries from clients
          poolSize  = cfg.otherNodes.size + 1,
          fetchSize = cfg.sqliteFetchSize
        )

      clients <-
        cfg.otherNodes.traverse: n =>
          EmberClientBuilder.default[IO].build.map(RpcClient(nodeId = n, retryAfter = 5.seconds, _))

      rn <-
        RaftNode
          .builder(cfg.thisNode, StateMachine(_, _), log, persist)
          .withExternals(clients*)
          .withElectionTimeout(500.millis, 1000.millis)
          .withHeartbeatEvery(100.millis)
          .build
    yield rn

  def server(cfg: KvStoreConfig, rn: RaftNode[IO, KvCommand, KvResponse, KvState]): IO[Nothing] =
    EmberServerBuilder
      .default[IO]
      .withHost(ipv4"0.0.0.0")
      .withPort(cfg.httpPort)
      .withHttp2
      .withHttpApp(Routes(rn).orNotFound)
      .build
      .use(_ => IO.never)

  def run: IO[Unit] =
    for
      given Logger[IO] <-
        Slf4jLogger.fromName[IO]("KvStore")

      cfg <- KvStoreConfig.loadFromEnv[IO]
      _   <- raftNode(cfg).use(server(cfg, _))
    yield ()
