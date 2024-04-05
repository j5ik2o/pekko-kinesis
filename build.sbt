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
  crossScalaVersions := Seq(Versions.scala213Version, Versions.scala3Version),
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
    logback.classic     % Test,
    scalatest.scalatest % Test
  ),
  libraryDependencies ++= Seq(
    apache.pekko.actor,
    apache.pekko.slf4j,
    apache.pekko.stream,
    "com.github.j5ik2o" %% "docker-controller-scala-scalatest" % "1.15.34" % Test excludeAll (ExclusionRule(
      organization = "org.slf4j"
    )),
    "com.github.j5ik2o" %% "docker-controller-scala-localstack" % "1.15.34" % Test excludeAll (ExclusionRule(
      organization = "org.slf4j"
    )),
    apache.pekko.testkit       % Test,
    apache.pekko.streamTestkit % Test
  ),
  Test / fork := true,
  Test / envVars := Map("AWS_CBOR_DISABLE" -> "1")
)

val `pekko-kinesis-kpl` = (project in file("pekko-kinesis-kpl"))
  .settings(baseSettings, dependenciesCommonSettings)
  .settings(
    name := "pekko-kinesis-kpl",
    libraryDependencies ++= Seq(
      amazonAws.kinesisProducer,
      amazonAws.kinesis    % Test,
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
      amazonAws.KCLv1,
      amazonAws.streamKinesisAdaptor % Test,
      amazonAws.kinesis              % Test,
      amazonAws.cloudwatch           % Test,
      amazonAws.dynamodb             % Test
    ),
    libraryDependencies ++= Seq(
      iheart.ficus,
      scalaLang.scalaJava8Compat
    ),
    Test / parallelExecution := false
  )

val `pekko-kinesis-kcl-v2` = (project in file("pekko-kinesis-kcl-v2"))
  .settings(baseSettings, dependenciesCommonSettings)
  .settings(
    name := "pekko-kinesis-kcl-v2",
    libraryDependencies ++= Seq(
      softwareAmazon.KCLv2,
      softwareAmazon.kinesis    % Test,
      softwareAmazon.cloudwatch % Test,
      softwareAmazon.dynamodb   % Test
    ),
    libraryDependencies ++= Seq(
      iheart.ficus,
      scalaLang.scalaJava8Compat
    ),
    Test / parallelExecution := false
  )

val `pekko-kinesis-kcl-dynamodb-streams` = (project in file("pekko-kinesis-kcl-dynamodb-streams"))
  .settings(baseSettings, dependenciesCommonSettings)
  .settings(
    name := "pekko-kinesis-kcl-dynamodb-streams",
    libraryDependencies ++= Seq(
      amazonAws.KCLv1,
      amazonAws.dynamodb,
      amazonAws.streamKinesisAdaptor,
      amazonAws.kinesis    % Test,
      amazonAws.cloudwatch % Test,
      amazonAws.dynamodb   % Test
    ),
    libraryDependencies ++= Seq(
      iheart.ficus,
      scalaLang.scalaJava8Compat
    ),
    Test / parallelExecution := false
  ).dependsOn(`pekko-kinesis-kcl` % "compile->compile;test->test")

val `pekko-kinesis-root` = (project in file("."))
  .settings(baseSettings)
  .settings(name := "pekko-kinesis-root")
  .aggregate(`pekko-kinesis-kpl`, `pekko-kinesis-kcl`, `pekko-kinesis-kcl-v2`, `pekko-kinesis-kcl-dynamodb-streams`)

// --- Custom commands
addCommandAlias("lint", ";scalafmtCheck;test:scalafmtCheck;scalafmtSbtCheck;scalafixAll --check")
addCommandAlias("fmt", ";scalafmtAll;scalafmtSbt;scalafix RemoveUnused")
