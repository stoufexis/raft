import com.typesafe.sbt.packager.docker.*

ThisBuild / scalaVersion := "3.4.2"
ThisBuild / name         := "raft"
ThisBuild / version      := "0.1.0-SNAPSHOT"
ThisBuild / organization := "com.stoufexis.raft"

lazy val cats =
  Seq(
    "co.fs2"        %% "fs2-core"    % "3.10.2",
    "org.typelevel" %% "cats-core"   % "2.10.0",
    "org.typelevel" %% "cats-effect" % "3.5.4"
  )

lazy val log =
  Seq(
    "org.typelevel" %% "log4cats-core"   % "2.6.0",
    "org.typelevel" %% "log4cats-slf4j"  % "2.6.0",
    "ch.qos.logback" % "logback-classic" % "1.5.6"
  )

lazy val rpc =
  Seq(
    "org.http4s" %% "http4s-ember-client" % "0.23.28",
    "org.http4s" %% "http4s-ember-server" % "0.23.28",
    "org.http4s" %% "http4s-dsl"          % "0.23.28",
    "org.http4s" %% "http4s-core"         % "0.23.28",
    "org.http4s" %% "http4s-circe"        % "0.23.28",
    "io.circe"   %% "circe-core"          % "0.14.1",
    "io.circe"   %% "circe-generic"       % "0.14.1",
    "io.circe"   %% "circe-parser"        % "0.14.1"
  )

lazy val persist =
  Seq(
    "org.tpolecat" %% "doobie-core"   % "1.0.0-RC5",
    "org.tpolecat" %% "doobie-hikari" % "1.0.0-RC5",
    "org.xerial"    % "sqlite-jdbc"   % "3.46.1.0"
  )

lazy val binary =
  Seq(
    "org.scodec" %% "scodec-core" % "2.3.1"
  )

lazy val test =
  Seq(
    "org.scalameta" %% "munit" % "1.0.0" % Test
  )

lazy val commonCompileFlags =
  Seq(
    "-Ykind-projector:underscores",
    "-Wvalue-discard",
    "-Wunused:implicits",
    "-Wunused:explicits",
    "-Wunused:imports",
    "-Wunused:locals",
    "-Wunused:params",
    "-Wunused:privates",
    "-source:future"
  )

lazy val raft =
  project
    .in(file("modules/raft"))
    .settings(
      libraryDependencies ++= cats ++ log ++ test,
      scalacOptions ++= commonCompileFlags
    )

lazy val kvstore =
  project
    .enablePlugins(DockerPlugin, JavaAppPackaging)
    .in(file("modules/kvstore"))
    .dependsOn(raft)
    .settings(
      libraryDependencies ++= cats ++ log ++ rpc ++ binary ++ test ++ persist,
      scalacOptions ++= commonCompileFlags,
      // docker
      dockerBaseImage := "openjdk:11",
      dockerCommands += Cmd("USER", "root"),
      dockerCommands += Cmd("RUN", "mkdir", "/var/opt/sqlite"),
      dockerCommands += Cmd("RUN", "chown", (Docker / daemonUser).value, "/var/opt/sqlite"),
      dockerCommands += Cmd("USER", (Docker / daemonUser).value)
    )

lazy val root =
  project
    .in(file("."))
    .aggregate(raft, kvstore)
