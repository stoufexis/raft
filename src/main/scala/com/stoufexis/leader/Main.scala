package com.stoufexis.leader

import cats.effect.*
import cats.effect.std.Mutex
import cats.implicits.given
import fs2.*
import fs2.grpc.syntax.all.*
import io.grpc.ManagedChannel
import io.grpc.Metadata
import io.grpc.ServerServiceDefinition
import io.grpc.netty.shaded.io.grpc.netty.*

import com.stoufexis.leader.proto.protos.*
import com.stoufexis.leader.util.raceFirstOrError

import scala.annotation.unused
import scala.concurrent.duration.*

/*
  TODOS For leader

  Follow Raft's leader election for implementing the core logic.
  Assume one independent leader (and one independent leader election) per entity that you want to decide leadership over (eg per shard)
  Grouping rpc requests between the nodes should be a later optimization step

  Note that raft is vulnerable to network partitions. A leader might be in the minority of the partition
  and it may never learn that there is a new term and a new leader elected for that term.
  I could implement the following for that case:
 * The leader heartbeats get a response from the followers that contain the term of the follower. If the term received
      by the leader is larger than its own, then it demotes its self to follower.
 * If the leader cannot successfully successfully send heartbeats to the majority of the other nodes, it demotes its self
 * If we time the voting timeouts and heartbeats smartly, this can catch almost all cases
 * Note that long process pauses messes up this flow, since between receiving heartbeat responses and doing an action as a leader a long pause may happen
 * the term is always provided to the end user, so they can decide if they want to do something to handle this case
 * eg for event sourcing, the term is added to the event, and decreasing terms are ignored when replaying the events

    For event sourcing scenarios:
 * A stale leader can be allowed to write to the even sourcing persistence layer (ie a log table in postgres).
      If the fresh leader tries to write something in the log with a sequence number, it will fail because an even with that sequence number already exists
      then it must read the events the stale leader wrote and rebuild its state. If the stale leader continues to write for long, this will make the fresh
      leader very slow, because it will be re-reading the events, but we hope the stale leader will get demoted soon.
      In escence, until the stale leader gets demoted, we are losing the benefits of event sourcing and we go back to contention in the persistence layer.
      In this case the leadership layer is acting as a performance optimisation for having a single writer for a database

    Notes:
 * Theres a fundamental problem if a stale leader of an entity does an action against a fresh leader of another entity.
        If the writes of the stale leader are ignored, then the system stops being consistent.
        The event sourcing schenarios scetion above handles this case.

    Questionable ideas:
 * We provide the ability to the user to force heartbeats and succeed only if the rest of the nodes were contacted
 * Note this can still fail because of the process pausing for long. EG we receive heartbeat confirmation from other nodes
        then the process pauses for enough time that we are now stale, the process restarts and sends to the user that we are the leader
 * Lets say election timeout happens every 30 seconds (starting from the same beginning), i.e at 15:00 then 15:30 then 16:00.
      it is not an interval, its a cron schedule. Intermediary heartbeats are sent every 10 seconds. If 3 of them fail, then election is restarted.
      if the first or the second one fails, election is not restarted yet, but we can transition into a PotentiallyStale state and give the user
      this information, so they can hold of any commits for now.


  What about leadership balance?
 * If a node goes down and we follow normal raft leader election of first-come-first-serve, a single node will probably become the leader
    for all entities. The node may even come up later, but will be the leader of no entities. Thus, we need a way of rebalancing the leadership.
 * Kafka does it by moving leadership when it detects it is unbalanced. We can do it like that.
 * I propose the following. If a node that is acting as a follower detects a leader that has a partition it wants,
    it may ask for it by initiating a leader election, just like when it is detected a leader is down.
 * We can know if a follower wants an entity by assigning a preferred node to each entity, by using consistent hashing. IE consistent_hash(entity) -> node
 * The above assumes a constant node id for a member of the cluster.
 */

// class PingerServiceImpl extends PingerFs2Grpc[IO, Metadata]:
//   def ping(request: PingRequest, @unused ctx: Metadata): IO[PingResponse] =
//     request match
//       case PingRequest("PING") => IO.pure(PingResponse("PONG"))
//       case PingRequest(msg)    => IO.pure(PingResponse(s"Malformed request: $msg"))

// val client: IO[Unit] =
//   val managedChannelResource: Resource[IO, ManagedChannel] =
//     NettyChannelBuilder
//       .forAddress("127.0.0.1", 9999)
//       .usePlaintext()
//       .resource[IO]

//   val resource: Resource[IO, PingerFs2Grpc[IO, Metadata]] =
//     managedChannelResource
//       .flatMap(PingerFs2Grpc.stubResource)

//   resource.use: service =>
//     List(PingRequest("PING"), PingRequest("PiNG"))
//       .traverse: ping =>
//         for
//           response <- service.ping(ping, Metadata())
//           _        <- IO.println(response)
//         yield ()
//       .void

// val runServer: Resource[IO, Unit] =
//   for
//     service: ServerServiceDefinition <-
//       PingerFs2Grpc.bindServiceResource[IO](new PingerServiceImpl)

//     _ <-
//       NettyServerBuilder
//         .forPort(9999)
//         .addService(service)
//         .resource[IO]
//         .evalMap(server => IO(server.start()))
//   yield ()

// object Server extends IOApp.Simple:
//   def run: IO[Unit] = runServer.useForever

// object Client extends IOApp.Simple:
//   def run: IO[Unit] = client

object Main extends IOApp.Simple:
  val streams: Stream[IO, Int] =
    Stream.awakeDelay[IO](100.millis).evalTap(d => IO.println(d)).map(_ => 1).onFinalizeCase(IO.println)
      .interleaveAll(Stream.awakeDelay[IO](5.second).evalTap(d => IO.println(d)).map(_ =>
        2
      ).onFinalizeCase(IO.println))

  def run =
    for
      f <-
        streams.evalMap(d => IO.println("Done " + d)).compile.drain.start
      _ <- IO.readLine
      _ <- f.cancel
    yield ()
