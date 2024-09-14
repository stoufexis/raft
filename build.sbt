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

lazy val grpc =
  Seq(
    "io.grpc"               % "grpc-netty-shaded" % scalapb.compiler.Version.grpcJavaVersion,
    "com.thesamet.scalapb" %% "scalapb-runtime"   % scalapb.compiler.Version.scalapbVersion % "protobuf",
    "org.typelevel"        %% "fs2-grpc-codegen"  % "2.7.16",
    "org.typelevel"        %% "fs2-grpc-runtime"  % "2.7.16"
  )

lazy val persist =
  Seq(
    "org.tpolecat" %% "doobie-core" % "1.0.0-RC5",
    "org.xerial"    % "sqlite-jdbc" % "3.46.1.0"
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
    "-language:strictEquality",
    "-source:future"
  )

lazy val raft =
  project
    .in(file("modules/raft"))
    .settings(
      libraryDependencies ++= cats ++ log ++ test,
      scalacOptions ++= commonCompileFlags
    )

lazy val kvproto =
  project
    .in(file("modules/kvproto"))
    .enablePlugins(Fs2Grpc)
    .settings(
      libraryDependencies ++= grpc
    )

lazy val kvstore =
  project
    .in(file("modules/kvstore"))
    .dependsOn(kvproto, raft)
    .settings(
      libraryDependencies ++= cats ++ log ++ grpc ++ persist ++ test,
      scalacOptions ++= commonCompileFlags
    )

lazy val root =
  project
    .in(file("."))
    .aggregate(raft, kvproto, kvstore)
