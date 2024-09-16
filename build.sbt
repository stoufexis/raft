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

lazy val http4s =
  Seq(
    "org.http4s" %% "http4s-ember-client" % "0.23.28",
    "org.http4s" %% "http4s-ember-server" % "0.23.28",
    "org.http4s" %% "http4s-dsl"          % "0.23.28",
    "org.http4s" %% "http4s-core"         % "0.23.28",
    "org.http4s" %% "http4s-netty-server" % "0.5.19",
    "org.http4s" %% "http4s-netty-client" % "0.5.19"
  )

lazy val persist =
  Seq(
    "org.tpolecat" %% "doobie-core" % "1.0.0-RC5",
    "org.xerial"    % "sqlite-jdbc" % "3.46.1.0",
    "org.scodec"   %% "scodec-core" % "2.3.1"
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
    .in(file("modules/kvstore"))
    .dependsOn(raft)
    .settings(
      libraryDependencies ++= cats ++ log ++ http4s ++ test ++ persist,
      scalacOptions ++= commonCompileFlags
    )

lazy val root =
  project
    .in(file("."))
    .aggregate(raft, kvstore)
