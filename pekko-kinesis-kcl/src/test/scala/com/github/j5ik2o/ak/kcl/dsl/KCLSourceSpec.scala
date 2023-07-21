package com.github.j5ik2o.ak.kcl.dsl

import com.amazonaws.SDKGlobalConfiguration
import com.amazonaws.auth.{ AWSStaticCredentialsProvider, BasicAWSCredentials }
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.regions.Regions
import com.amazonaws.services.cloudwatch.{ AmazonCloudWatch, AmazonCloudWatchClientBuilder }
import com.amazonaws.services.dynamodbv2.{ AmazonDynamoDBAsync, AmazonDynamoDBAsyncClientBuilder }
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.{
  InitialPositionInStream,
  KinesisClientLibConfiguration
}
import com.amazonaws.services.kinesis.metrics.impl.NullMetricsFactory
import com.amazonaws.services.kinesis.model.{ PutRecordRequest, Record, ResourceNotFoundException }
import com.amazonaws.services.kinesis.{ AmazonKinesis, AmazonKinesisClientBuilder }
import com.github.j5ik2o.ak.kcl.util.KCLConfiguration
import com.github.j5ik2o.dockerController.localstack.{ LocalStackController, Service }
import com.github.j5ik2o.dockerController.{ DockerController, DockerControllerSpecSupport, WaitPredicates }
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.KillSwitches
import org.apache.pekko.stream.scaladsl.{ Keep, Sink }
import org.apache.pekko.testkit.TestKit
import org.scalatest.concurrent.{ Eventually, ScalaFutures }
import org.scalatest.freespec.AnyFreeSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{ Millis, Seconds, Span }

import java.net.InetAddress
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.UUID
import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.concurrent.duration.{ Duration, _ }
import scala.util.{ Failure, Success, Try }

class KCLSourceSpec
    extends TestKit(ActorSystem("KCLSourceSpec"))
    with AnyFreeSpecLike
    with Matchers
    with ScalaFutures
    with Eventually
    with DockerControllerSpecSupport {
  System.setProperty(SDKGlobalConfiguration.AWS_CBOR_DISABLE_SYSTEM_PROPERTY, "true")
  override protected val dockerControllers: Vector[DockerController] = Vector(controller)
  logger.debug(s"testTimeFactor = $testTimeFactor")
  override protected val waitPredicatesSettings: Map[DockerController, WaitPredicateSetting] =
    Map(
      controller -> WaitPredicateSetting(Duration.Inf, WaitPredicates.forLogMessageExactly("Ready."))
    )
  val testTimeFactor: Int          = sys.env.getOrElse("TEST_TIME_FACTOR", "1").toInt
  val region: Regions              = Regions.AP_NORTHEAST_1
  val accessKeyId: String          = "AKIAIOSFODNN7EXAMPLE"
  val secretAccessKey: String      = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
  val hostPort: Int                = temporaryServerPort()
  val endpointOfLocalStack: String = s"http://$dockerHost:$hostPort"
  val controller: LocalStackController =
    LocalStackController(
      dockerClient,
      envVars = Map(
        "DYNAMODB_SHARE_DB"  -> "1",
        "DYNAMODB_IN_MEMORY" -> "1"
      )
    )(
      services = Set(Service.DynamoDB, Service.Kinesis, Service.CloudWatch),
      edgeHostPort = hostPort,
      hostNameExternal = Some(dockerHost),
      defaultRegion = Some(region.getName)
    )

  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(
      timeout = Span(60 * testTimeFactor, Seconds),
      interval = Span(500 * testTimeFactor, Millis)
    )

  val applicationName: String = "kcl-source-spec"
  val streamName: String      = sys.env.getOrElse("STREAM_NAME", "kcl-flow-spec") + UUID.randomUUID().toString
  val workerId: String        = InetAddress.getLocalHost.getCanonicalHostName + ":" + UUID.randomUUID()

  var awsKinesisClient: AmazonKinesis                              = _
  var awsDynamoDBClient: AmazonDynamoDBAsync                       = _
  var awsCloudWatch: AmazonCloudWatch                              = _
  var kinesisClientLibConfiguration: KinesisClientLibConfiguration = _

  override def afterStartContainers(): Unit = {
    val credentialsProvider = new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKeyId, secretAccessKey))
    val dynamoDbEndpointConfiguration   = new EndpointConfiguration(endpointOfLocalStack, region.getName)
    val kinesisEndpointConfiguration    = new EndpointConfiguration(endpointOfLocalStack, region.getName)
    val cloudwatchEndpointConfiguration = new EndpointConfiguration(endpointOfLocalStack, region.getName)

    awsDynamoDBClient = AmazonDynamoDBAsyncClientBuilder
      .standard()
      .withCredentials(credentialsProvider)
      .withEndpointConfiguration(dynamoDbEndpointConfiguration)
      .build()

    awsKinesisClient = AmazonKinesisClientBuilder
      .standard()
      .withCredentials(credentialsProvider)
      .withEndpointConfiguration(kinesisEndpointConfiguration)
      .build()

    awsCloudWatch = AmazonCloudWatchClientBuilder
      .standard()
      .withCredentials(credentialsProvider)
      .withEndpointConfiguration(cloudwatchEndpointConfiguration).build()

    val httpStatusCode = awsKinesisClient.createStream(streamName, 1).getSdkHttpMetadata.getHttpStatusCode
    assert(httpStatusCode == 200)
    waitStreamToCreated(streamName)

    kinesisClientLibConfiguration = KCLConfiguration.fromConfig(
      system.settings.config,
      applicationName,
      UUID.randomUUID(),
      streamName,
      credentialsProvider,
      credentialsProvider,
      credentialsProvider,
      configOverrides = Some(
        KCLConfiguration.ConfigOverrides(positionInStreamOpt = Some(InitialPositionInStream.TRIM_HORIZON))
      )
    )

  }

  def waitStreamToCreated(streamName: String, waitDuration: Duration = 1.seconds): Try[Unit] = {
    @tailrec
    def go: Try[Unit] = {
      Try { awsKinesisClient.describeStream(streamName) } match {
        case Success(result) if result.getStreamDescription.getStreamStatus == "ACTIVE" =>
          println(s"waiting completed: $streamName, $result")
          Success(())
        case Failure(_: ResourceNotFoundException) =>
          Thread.sleep(waitDuration.toMillis * sys.env.getOrElse("TEST_TIME_FACTOR", "1").toInt)
          println("waiting until stream creates")
          go
        case Failure(ex) => Failure(ex)
        case _ =>
          Thread.sleep(waitDuration.toMillis * sys.env.getOrElse("TEST_TIME_FACTOR", "1").toInt)
          println("waiting until stream creates")
          go

      }
    }
    val result = go
    Thread.sleep(waitDuration.toMillis * sys.env.getOrElse("TEST_TIME_FACTOR", "1").toInt)
    result
  }

  def waitStreamsToCreated(): Try[Unit] = {
    Try { awsKinesisClient.listStreams() }.flatMap { result =>
      result.getStreamNames.asScala.foldLeft(Try(())) { case (_, streamName) =>
        waitStreamToCreated(streamName)
      }
    }
  }

  import system.dispatcher

  "KCLSourceSpec" - {
    "should be able to consume message" in {
      var result: Record = null
      val (sw, future) = KCLSource
        .withoutCheckpoint(
          kinesisClientLibConfiguration,
          amazonKinesisOpt = Some(awsKinesisClient),
          amazonDynamoDBOpt = Some(awsDynamoDBClient),
          amazonCloudWatchOpt = Some(awsCloudWatch),
          iMetricsFactoryOpt = Some(new NullMetricsFactory)
        ).viaMat(KillSwitches.single)(Keep.right)
        .map { msg =>
          result = msg.record
          msg
        }
        .via(KCLFlow.ofCheckpoint())
        .toMat(Sink.ignore)(Keep.both)
        .run()

      val text = "abc"
      awsKinesisClient.putRecord(
        new PutRecordRequest()
          .withStreamName(streamName)
          .withPartitionKey("k-1")
          .withData(ByteBuffer.wrap(text.getBytes(StandardCharsets.UTF_8)))
      )

      eventually {
        assert(result != null && new String(result.getData.array()) == text)
      }

      sw.shutdown()
      future.futureValue

    }
  }
}
