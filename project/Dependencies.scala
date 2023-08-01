import sbt._

object Dependencies {

  object Versions {
    val scala212Version            = "2.12.13"
    val scala213Version            = "2.13.11"
    val scala3Version              = "3.3.0"
    val awsSdkVersion              = "1.12.511"
    val pekkoVersion               = "1.0.0"
    val testcontainersScalaVersion = "0.40.14"
    val scalaTestVersion           = "3.2.9"
    val logbackVersion             = "1.2.12"
  }

  object apache {

    object pekko {
      val actor         = "org.apache.pekko" %% "pekko-actor"          % Versions.pekkoVersion
      val slf4j         = "org.apache.pekko" %% "pekko-slf4j"          % Versions.pekkoVersion
      val stream        = "org.apache.pekko" %% "pekko-stream"         % Versions.pekkoVersion
      val testkit       = "org.apache.pekko" %% "pekko-testkit"        % Versions.pekkoVersion
      val streamTestkit = "org.apache.pekko" %% "pekko-stream-testkit" % Versions.pekkoVersion
    }
  }

  object amazonAws {
    val kinesis              = "com.amazonaws" % "aws-java-sdk-kinesis"             % Versions.awsSdkVersion
    val cloudwatch           = "com.amazonaws" % "aws-java-sdk-cloudwatch"          % Versions.awsSdkVersion
    val dynamodb             = "com.amazonaws" % "aws-java-sdk-dynamodb"            % Versions.awsSdkVersion
    val kinesisProducer      = "com.amazonaws" % "amazon-kinesis-producer"          % "0.14.12"
    val kinesisClient        = "com.amazonaws" % "amazon-kinesis-client"            % "1.13.3"
    val streamKinesisAdaptor = "com.amazonaws" % "dynamodb-streams-kinesis-adapter" % "1.5.1"
  }

  object iheart {
    val ficus = "com.iheart" %% "ficus" % "1.5.2"
  }

  object scalaLang {
    val scalaJava8Compat = "org.scala-lang.modules" %% "scala-java8-compat" % "1.0.2"
  }

  object dimafeng {

    val testcontainersScalatest =
      "com.dimafeng" %% "testcontainers-scala-scalatest" % Versions.testcontainersScalaVersion

    val testcontainersLocalstack =
      "com.dimafeng" %% "testcontainers-scala-localstack" % Versions.testcontainersScalaVersion
  }

  object scalatest {
    val scalatest = "org.scalatest" %% "scalatest" % Versions.scalaTestVersion
  }

  object logback {
    val classic = "ch.qos.logback" % "logback-classic" % Versions.logbackVersion

  }

}
