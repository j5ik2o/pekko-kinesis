# pekko-kinesis

[![CI](https://github.com/j5ik2o/pekko-kinesis/workflows/CI/badge.svg)](https://github.com/j5ik2o/pekko-kinesis/actions?query=workflow%3ACI)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.github.j5ik2o/pekko-kinesis-kcl_2.13/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.github.j5ik2o/pekko-kinesis-kcl_2.13)
[![Renovate](https://img.shields.io/badge/renovate-enabled-brightgreen.svg)](https://renovatebot.com)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

pekko-kinesis supports Pekko commponets for AWS Kinesis.

Forked from [akka-kinesis](https://github.com/j5ik2o/akka-kinesis).

## Support features

- KPLFlow
- KCLSource
- KCLSourceOnDynamoDBStreams (for DynamoDB Streams)

## Installation

Add the following to your sbt build (2.12.x, 2.13.x):

```scala
// if snapshot
resolvers += "Sonatype OSS Snapshot Repository" at "https://oss.sonatype.org/content/repositories/snapshots/"

val version = "..."

libraryDependencies += Seq(
  "com.github.j5ik2o" %% "pekko-kinesis-kcl" % version, // for KCL
  "com.github.j5ik2o" %% "pekko-kinesis-kpl" % version, // for KPL
  "com.github.j5ik2o" %% "pekko-kinesis-kcl-dynamodb-streams" % version // for KCL with DynamoDB Streams
)
```
