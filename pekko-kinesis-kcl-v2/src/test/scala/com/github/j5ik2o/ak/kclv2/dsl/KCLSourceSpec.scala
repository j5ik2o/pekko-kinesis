package com.github.j5ik2o.ak.kclv2.dsl

import com.amazonaws.SDKGlobalConfiguration
import com.amazonaws.regions.Regions
import com.github.j5ik2o.ak.kclv2.stage.KCLSourceStage
import com.github.j5ik2o.dockerController.localstack.{LocalStackController, Service}
import com.github.j5ik2o.dockerController.{DockerController, DockerControllerSpecSupport, WaitPredicates}
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.KillSwitches
import org.apache.pekko.stream.scaladsl.{Keep, Sink}
import org.apache.pekko.testkit.TestKit
import org.scalatest.concurrent.PatienceConfiguration.{Interval, Timeout}
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.freespec.AnyFreeSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, AwsCredentialsProvider, StaticCredentialsProvider}
import software.amazon.awssdk.core.{SdkBytes, SdkSystemSetting}
import software.amazon.awssdk.endpoints.Endpoint
import software.amazon.awssdk.http.{Protocol, SdkHttpConfigurationOption}
import software.amazon.awssdk.http.apache.ApacheHttpClient
import software.amazon.awssdk.http.async.SdkAsyncHttpClient
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient
import software.amazon.awssdk.services.cloudwatch.endpoints.{CloudWatchEndpointParams, CloudWatchEndpointProvider}
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.endpoints.{DynamoDbEndpointParams, DynamoDbEndpointProvider}
import software.amazon.awssdk.services.kinesis.endpoints.{KinesisEndpointParams, KinesisEndpointProvider}
import software.amazon.awssdk.services.kinesis.model.{CreateStreamRequest, DescribeStreamRequest, PutRecordRequest, ResourceNotFoundException, StreamStatus}
import software.amazon.awssdk.services.kinesis.{DefaultKinesisAsyncClientBuilder, KinesisAsyncClient, KinesisAsyncClientBuilder}
import software.amazon.awssdk.utils.AttributeMap
import software.amazon.kinesis.checkpoint.CheckpointConfig
import software.amazon.kinesis.common.{InitialPositionInStream, InitialPositionInStreamExtended}
import software.amazon.kinesis.leases.LeaseManagementConfig
import software.amazon.kinesis.retrieval.RetrievalConfig
import software.amazon.kinesis.retrieval.polling.PollingConfig

import java.net.{InetAddress, URI, URL}
import java.nio.charset.StandardCharsets
import java.util.UUID
import java.util.concurrent.{CompletableFuture, Executors}
import scala.annotation.tailrec
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{Duration, DurationInt}
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

class KCLSourceSpec extends TestKit(ActorSystem("KCLSourceSpec"))
with AnyFreeSpecLike
with Matchers
with ScalaFutures
with Eventually
with DockerControllerSpecSupport {
  System.setProperty(SdkSystemSetting.CBOR_ENABLED.property(), "false")
  System.setProperty(SDKGlobalConfiguration.AWS_CBOR_DISABLE_SYSTEM_PROPERTY, "true")
  val testTimeFactor: Int = sys.env.getOrElse("TEST_TIME_FACTOR", "1").toInt
  logger.debug(s"testTimeFactor = $testTimeFactor")

  val region               = Region.AP_NORTHEAST_1
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
      defaultRegion = Some(region.toString)
    )

  override protected val dockerControllers: Vector[DockerController] = Vector(controller)
  override protected val waitPredicatesSettings: Map[DockerController, WaitPredicateSetting] =
    Map(
      controller -> WaitPredicateSetting(Duration.Inf, WaitPredicates.forLogMessageExactly("Ready."))
    )

  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(
      timeout = Span(60 * testTimeFactor, Seconds),
      interval = Span(500 * testTimeFactor, Millis)
    )

  val applicationName: String = "kcl-v2-source-spec"
  val streamName: String      = sys.env.getOrElse("STREAM_NAME", "kcl-flow-spec") + UUID.randomUUID().toString
  val workerId: String        = InetAddress.getLocalHost.getCanonicalHostName + ":" + UUID.randomUUID()

  var awsKinesisClient: KinesisAsyncClient                              = _
  var awsDynamoDBClient: DynamoDbAsyncClient                       = _
  var awsCloudWatchClient: CloudWatchAsyncClient                              = _


  override def afterStartContainers(): Unit = {
    val credentialsProvider = StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKeyId, secretAccessKey))

    val nettyClient: SdkAsyncHttpClient =
      NettyNioAsyncHttpClient
        .builder()
        .protocol(Protocol.HTTP1_1)
        .build()

    awsDynamoDBClient = DynamoDbAsyncClient.builder()
      .httpClient(nettyClient)
      .credentialsProvider(credentialsProvider)
      .endpointOverride(new URI(endpointOfLocalStack)).region(region).build()

    awsKinesisClient = KinesisAsyncClient.builder()
      .httpClient(nettyClient)
      .credentialsProvider(credentialsProvider)
      .endpointOverride(new URI(endpointOfLocalStack)).region(region).build()

    awsCloudWatchClient = CloudWatchAsyncClient.builder()
      .httpClient(nettyClient)
      .credentialsProvider(credentialsProvider)
      .endpointOverride(new URI(endpointOfLocalStack)).region(region).build()

    val result  = awsKinesisClient
      .createStream(CreateStreamRequest.builder().streamName(streamName).shardCount(1).build()).join()
    val httpStatusCode = result.sdkHttpResponse().statusCode()
    assert(httpStatusCode == 200)
    waitStreamToCreated(streamName)
    Thread.sleep(1000L)

  }

  def waitStreamToCreated(streamName: String, waitDuration: Duration = 1.seconds): Try[Unit] = {
    @tailrec
    def go: Try[Unit] = {
      Try { awsKinesisClient.describeStream(DescribeStreamRequest.builder().streamName(streamName).build()).get() } match {
        case Success(result) if result.streamDescription().streamStatus == StreamStatus.ACTIVE =>
          println(s"waiting completed: $streamName, $result")
          Success(())
        case Failure(ex: ResourceNotFoundException) =>
          ex.printStackTrace()
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
    Try { awsKinesisClient.listStreams().get() }.flatMap { result =>
      result.streamNames.asScala.foldLeft(Try(())) { case (_, streamName) =>
        waitStreamToCreated(streamName)
      }
    }
  }

  import system.dispatcher

  "KCLSourceSpec" - {
    "should be able to consume message" in {
      var committableRecord: KCLSourceStage.CommittableRecord = null


      val configsBuilderFactory = KCLSourceStage.configsBuilderFactory(streamName, applicationName, awsKinesisClient, awsDynamoDBClient, awsCloudWatchClient, UUID.randomUUID().toString)
      val schedulerFactory = KCLSourceStage.schedulerFactory(
        leaseManagementConfigF = Some({ config: LeaseManagementConfig => config.initialPositionInStream(InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON)) }),
     //   retrievalConfigF = Some({ config: RetrievalConfig => config.retrievalSpecificConfig(new PollingConfig(streamName, awsKinesisClient))})
      )

      val executor = Executors.newSingleThreadExecutor()
      val ec = ExecutionContext.fromExecutor(executor)

      val (sw, future) = KCLSource(configsBuilderFactory, schedulerFactory)(ec)
        .viaMat(KillSwitches.single)(Keep.right)
        .map { msg =>
          committableRecord = msg
          msg
        }
        .via(KCLFlow.ofCheckpoint())
        .toMat(Sink.ignore)(Keep.both)
        .run()

      Thread.sleep(1000*3)

      val text = "abc"
      logger.debug("---->")
      val result = awsKinesisClient.putRecord(
        PutRecordRequest.builder()
          .streamName(streamName)
          .partitionKey("k-1")
          .data(SdkBytes.fromByteArray(text.getBytes(StandardCharsets.UTF_8))).build()
      ).join()
      assert(result.sdkHttpResponse().statusCode() == 200)
      logger.debug("<----")


      eventually(Timeout(30.seconds), Interval(1.second)) {
        assert(committableRecord != null )
      }

      sw.shutdown()
      future.futureValue
    }
  }
}
