package com.stoufexis.raft.kvstore

import cats.effect.*
import com.comcast.ip4s.*
import org.http4s.ember.client.EmberClientBuilder
import org.http4s.ember.server.EmberServerBuilder
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import com.stoufexis.raft.RaftNode
import com.stoufexis.raft.kvstore.persist.SqlitePersistence
import com.stoufexis.raft.kvstore.rpc.*
import com.stoufexis.raft.kvstore.statemachine.*

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
          poolSize  = cfg.otherNodes.size + 1
        )

      clients <-
        EmberClientBuilder
          .default[IO]
          .build
          .map(cl => cfg.otherNodes.map(RpcClient(_, cfg.clientRetryAfter, cl)))

      rn <-
        RaftNode
          .builder(cfg.thisNode, StateMachine(_, _), log, persist)
          .withExternals(clients*)
          .withElectionTimeout(cfg.electionTimeoutLow, cfg.electionTimeoutHigh)
          .withHeartbeatEvery(cfg.heartbeatEvery)
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
