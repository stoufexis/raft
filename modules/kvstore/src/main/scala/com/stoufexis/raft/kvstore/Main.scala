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

object Main extends IOApp.Simple:
  def raftNode(cfg: KvStoreConfig): Resource[IO, RaftNode[IO, KvCommand, KvResponse, KvState]] =
    for
      given Logger[IO] <-
        Resource.eval(Slf4jLogger.fromName[IO]("KvStore"))

      (log, persist) <-
        SqlitePersistence[IO, KvCommand](cfg.sqliteDbPath, cfg.sqliteFetchSize)

      clients <-
        cfg.otherNodes.traverse: n =>
          EmberClientBuilder.default[IO].build.map(RpcClient(n, _))

      rn <-
        RaftNode
          .builder(cfg.thisNode, StateMachine(_, _), log, persist)
          .withExternals(clients*)
          .build
    yield rn

  def serverFromRaft(rn: RaftNode[IO, KvCommand, KvResponse, KvState]): IO[Nothing] =
    val routes: Routes[IO] =
      Routes[IO](rn)

    val clientFacing: IO[Nothing] =
      EmberServerBuilder
        .default[IO]
        .withHost(ipv4"0.0.0.0")
        .withPort(port"8080")
        .withHttp2
        .withHttpApp(routes.clientRoutes.orNotFound)
        .build
        .use(_ => IO.never)

    val internal: IO[Nothing] =
      EmberServerBuilder
        .default[IO]
        .withHost(ipv4"0.0.0.0")
        .withPort(port"8081")
        .withHttp2
        .withHttpApp(routes.raftRoutes.orNotFound)
        .build
        .use(_ => IO.never)

    IO.race(clientFacing, internal) >> IO.never

  def run: IO[Unit] =
    for
      cfg <- KvStoreConfig.loadFromEnv[IO]
      _   <- raftNode(cfg).use(serverFromRaft)
    yield ()
