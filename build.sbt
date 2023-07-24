import Dependencies._

def crossScalacOptions(scalaVersion: String): Seq[String] =
  CrossVersion.partialVersion(scalaVersion) match {
    case Some((3L, _)) =>
      Seq(
        "-source:3.0-migration",
        "-Xignore-scala2-macros"
      )
    case Some((2L, scalaMajor)) if scalaMajor >= 12 =>
      Seq(
        "-Ydelambdafy:method",
        "-target:jvm-1.8",
        "-Yrangepos",
        "-Ywarn-unused"
      )
  }

lazy val baseSettings = Seq(
  organization := "com.github.j5ik2o",
  homepage := Some(url("https://github.com/j5ik2o/pekko-kinesis")),
  licenses := List("The MIT License" -> url("http://opensource.org/licenses/MIT")),
  developers := List(
    Developer(
      id = "j5ik2o",
      name = "Junichi Kato",
      email = "j5ik2o@gmail.com",
      url = url("https://blog.j5ik2o.me")
    )
  ),
  scalaVersion := Versions.scala213Version,
  crossScalaVersions := Seq(Versions.scala213Version),
  scalacOptions ++= (Seq(
    "-unchecked",
    "-feature",
    "-deprecation",
    "-encoding",
    "UTF-8",
    "-language:_"
  ) ++ crossScalacOptions(scalaVersion.value)),
  resolvers ++= Resolver.sonatypeOssRepos("snapshots") ++ Seq(
    "Seasar Repository" at "https://maven.seasar.org/maven2/"
  ),
  Test / publishArtifact := false,
  Test / parallelExecution := false,
  Compile / doc / sources := {
    val old = (Compile / doc / sources).value
    if (scalaVersion.value == Versions.scala3Version) {
      Nil
    } else {
      old
    }
  },
  semanticdbEnabled := true,
  semanticdbVersion := scalafixSemanticdb.revision,
  // Remove me when scalafix is stable and feature-complete on Scala 3
  ThisBuild / scalafixScalaBinaryVersion := (CrossVersion.partialVersion(scalaVersion.value) match {
    case Some((2, _)) => CrossVersion.binaryScalaVersion(scalaVersion.value)
    case _            => CrossVersion.binaryScalaVersion(Versions.scala212Version)
  }),
  envVars := Map(
    "AWS_REGION" -> "ap-northeast-1"
  )
)

val dependenciesCommonSettings = Seq(
  libraryDependencies ++= Seq(
    amazonAws.kinesis,
    logback.classic     % Test,
    scalatest.scalatest % Test
  ),
  libraryDependencies ++= Seq(
    apache.pekko.actor excludeAll (ExclusionRule(organization = "org.scala-lang.modules")),
    apache.pekko.slf4j,
    apache.pekko.stream,
    dimafeng.testcontainersScalatest  % Test,
    dimafeng.testcontainersLocalstack % Test,
    "com.github.j5ik2o"              %% "docker-controller-scala-scalatest"  % "1.15.0" % Test,
    "com.github.j5ik2o"              %% "docker-controller-scala-localstack" % "1.15.0" % Test,
    apache.pekko.testkit              % Test,
    apache.pekko.streamTestkit        % Test
  ).map(_.cross(CrossVersion.for3Use2_13)),
  Test / fork := true,
  Test / envVars := Map("AWS_CBOR_DISABLE" -> "1")
)

val `pekko-kinesis-kpl` = (project in file("pekko-kinesis-kpl"))
  .settings(baseSettings, dependenciesCommonSettings)
  .settings(
    name := "pekko-kinesis-kpl",
    libraryDependencies ++= Seq(
      amazonAws.kinesisProducer,
      amazonAws.cloudwatch % Test,
      amazonAws.dynamodb   % Test
    ),
    Test / parallelExecution := false
  )

val `pekko-kinesis-kcl` = (project in file("pekko-kinesis-kcl"))
  .settings(baseSettings, dependenciesCommonSettings)
  .settings(
    name := "pekko-kinesis-kcl",
    libraryDependencies ++= Seq(
      amazonAws.kinesisClient,
      amazonAws.streamKinesisAdaptor % Test,
      amazonAws.cloudwatch           % Test,
      amazonAws.dynamodb             % Test
    ),
    libraryDependencies ++= Seq(
      iheart.ficus,
      scalaLang.scalaJava8Compat
    ).map(_.cross(CrossVersion.for3Use2_13)),
    Test / parallelExecution := false
  )

val `pekko-kinesis-kcl-dynamodb-streams` = (project in file("pekko-kinesis-kcl-dynamodb-streams"))
  .settings(baseSettings, dependenciesCommonSettings)
  .settings(
    name := "pekko-kinesis-kcl-dynamodb-streams",
    libraryDependencies ++= Seq(
      amazonAws.kinesisClient,
      amazonAws.dynamodb,
      amazonAws.streamKinesisAdaptor,
      amazonAws.cloudwatch % Test,
      amazonAws.dynamodb   % Test
    ),
    libraryDependencies ++= Seq(
      iheart.ficus,
      scalaLang.scalaJava8Compat
    ).map(_.cross(CrossVersion.for3Use2_13)),
    Test / parallelExecution := false
  ).dependsOn(`pekko-kinesis-kcl` % "compile->compile;test->test")

val `pekko-kinesis-root` = (project in file("."))
  .settings(baseSettings)
  .settings(name := "pekko-kinesis-root")
  .aggregate(`pekko-kinesis-kpl`, `pekko-kinesis-kcl`, `pekko-kinesis-kcl-dynamodb-streams`)

// --- Custom commands
addCommandAlias("lint", ";scalafmtCheck;test:scalafmtCheck;scalafmtSbtCheck;scalafixAll --check")
addCommandAlias("fmt", ";scalafmtAll;scalafmtSbt;scalafix RemoveUnused")
