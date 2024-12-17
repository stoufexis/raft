//> using scala 3.3
//> using toolkit typelevel:default
//> using dep "io.circe::circe-generic:0.14.10"

import cats.effect.*
import cats.implicits.given
import cats.effect.implicits.given
import org.http4s.client.*
import org.http4s.ember.client.*
import org.http4s.circe.*
import org.http4s.ember.client.given
import java.util.UUID
import io.circe.*
import org.http4s.*
import cats.effect.std.Random
import org.http4s.client.middleware.FollowRedirect

object concurrent_read_writes extends IOApp:
  case class PutResponse(status: String) derives Decoder:
    def successOrRaise: IO[Unit] =
      IO.raiseUnless(status == "SUCCESS")(RuntimeException(s"Bad response: $status"))

  case class ValuesResponse(status: String, revision_id: String, values: Json) derives Decoder:
    def getKeyOrRaise[A: Decoder](key: String): IO[A] =
      IO.fromEither(values.hcursor.get[A](key))

  given [A: Decoder, B: Decoder]: Decoder[Either[A, B]] =
    Decoder[A].map(Left(_)) or Decoder[B].map(Right(_))

  def uri(str: String): Uri =
    Uri.fromString(str).getOrElse(???)

  def getRequest(keys: String*): Request[IO] =
    getRequest(keys.toList)

  def getRequest(keys: List[String]): Request[IO] =
    val keysString: String =
      keys.map(k => s"keys=$k").mkString("&")

    Request(
      method = Method.GET,
      uri = uri(s"http://localhost:8080/store?command_id=${UUID.randomUUID.toString}&$keysString")
    )

  def putRequest(keys: (String, String)*): Request[IO] =
    putRequest(keys.toMap)

  def putRequest(keys: Map[String, String]): Request[IO] =
    val keysString: String =
      keys.map((k,v) => s"$k=$v").mkString("&")

    Request(
      method = Method.PUT,
      uri = uri(s"http://localhost:8080/store?command_id=${UUID.randomUUID.toString}&$keysString")
    )

  def putTxRequest(rid: String, keys: (String, String)*): Request[IO] =
    putTxRequest(rid, keys.toMap)

  def putTxRequest(rid: String, keys: Map[String, String]): Request[IO] =
    val keysString: String =
      keys.map((k,v) => s"$k=$v").mkString("&")

    Request(
      method = Method.PUT,
      uri = uri(s"http://localhost:8080/store/tx?revision_id=$rid&command_id=${UUID.randomUUID.toString}&$keysString")
    )

  def initialize(keyspace: Range, initVal: Int)(using client: Client[IO]): IO[Unit] =
    val keys: Map[String, String] =
      keyspace.map(num => (s"key$num", s"$initVal")).toMap

    for
      response <- client.expect(putRequest(keys))(jsonOf[IO, PutResponse])
      _        <- response.successOrRaise >> IO.println(s"Initialized keys with value $initVal")
    yield ()

  def runSingleReadWriteTx(keyNum: Int)(using client: Client[IO]): IO[Unit] =
    val key = s"key$keyNum"

    def loop(response: ValuesResponse): IO[Unit] =
      response.getKeyOrRaise[Int](key).flatMap: value =>
        val put: IO[Either[ValuesResponse, PutResponse]] =
          client.expect(putTxRequest(response.revision_id, key -> s"${value + 1}"))(jsonOf[IO, Either[ValuesResponse, PutResponse]])

        put.flatMap:
          case Left(values)   => loop(values)
          case Right(putResp) => putResp.successOrRaise

    client
      .expect(getRequest(key))(jsonOf[IO, ValuesResponse])
      .flatMap(loop(_))

  def runSingleReadWrite(keyNum: Int)(using client: Client[IO]): IO[Unit] =
    val key = s"key$keyNum"

    for
      response: ValuesResponse <-
        client.expect(getRequest(key))(jsonOf[IO, ValuesResponse])

      value: Int <-
        response.getKeyOrRaise[Int](key)

      response: PutResponse <-
        client.expect(putRequest(key -> s"${value + 1}"))(jsonOf[IO, PutResponse])

      _ <- response.successOrRaise
    yield ()

  def inspect(keyspace: Range)(using client: Client[IO]): IO[Unit] =
    val keys: List[String] =
      keyspace.map(num => s"key$num").toList

    for
      response <- client.expect(getRequest(keys))(jsonOf[IO, ValuesResponse])
      _        <- IO.println("Final values:")
      _        <- IO.println(response.values)
    yield ()

  def run(args: List[String]): IO[ExitCode] =
    val tx: Boolean =
      args.head match {
        case "tx"   => true
        case "notx" => false
      }

    val kspc: Int =
      args.tail.head.toInt

    val keySpace = (1 to kspc)

    val workersCnt: Int =
      args.tail.tail.head.toInt

    def workers(using client: Client[IO]): List[IO[Unit]] =
      keySpace.toList.flatMap: keyNum =>
        List.fill(workersCnt)(if (tx) runSingleReadWriteTx(keyNum) else runSingleReadWrite(keyNum))

    def runShuffled(using client: Client[IO]): IO[Unit] =
      for
        rnd <- Random.scalaUtilRandom[IO]
        ws  <- rnd.shuffleList(workers)
        _   <- IO.println(s"Running ${ws.length} workers")
        _   <- ws.parSequence_
      yield ()


    EmberClientBuilder.default[IO].build.use: client =>
      given Client[IO] = FollowRedirect(100)(client)

      initialize(keySpace, 1) >> runShuffled >> inspect(keySpace) as ExitCode.Success