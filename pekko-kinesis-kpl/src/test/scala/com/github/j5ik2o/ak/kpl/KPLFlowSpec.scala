package com.github.j5ik2o.ak.kpl

import java.nio.ByteBuffer
import java.util.UUID
import java.util.concurrent.TimeUnit
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.{ Keep, Sink, Source }
import org.apache.pekko.stream.{ ActorMaterializer, KillSwitches }
import org.apache.pekko.testkit.TestKit
import com.amazonaws.SDKGlobalConfiguration
import com.amazonaws.auth.{ AWSStaticCredentialsProvider, BasicAWSCredentials }
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.regions.Regions
import com.amazonaws.services.kinesis.model.ResourceNotFoundException
import com.amazonaws.services.kinesis.producer.{ KinesisProducerConfiguration, UserRecord, UserRecordResult }
import com.amazonaws.services.kinesis.{ AmazonKinesis, AmazonKinesisClientBuilder }
import com.github.dockerjava.api.command.RemoveContainerCmd
import com.github.j5ik2o.ak.kpl.dsl.{ KPLFlow, KPLFlowSettings }
import com.github.j5ik2o.dockerController.{ DockerController, DockerControllerSpecSupport, WaitPredicates }
import com.github.j5ik2o.dockerController.localstack.{ LocalStackController, Service }
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.{ Eventually, ScalaFutures }
import org.scalatest.freespec.AnyFreeSpecLike
import org.scalatest.matchers.should.Matchers

import scala.annotation.tailrec
import scala.jdk.CollectionConverters._
import scala.concurrent.duration.{ Duration, _ }
import scala.util.{ Failure, Success, Try }

class KPLFlowSpec
    extends TestKit(ActorSystem("KPLFlowSpec"))
    with AnyFreeSpecLike
    with BeforeAndAfterAll
    with Matchers
    with ScalaFutures
    with DockerControllerSpecSupport
    with Eventually {

  System.setProperty(SDKGlobalConfiguration.AWS_CBOR_DISABLE_SYSTEM_PROPERTY, "true")
  System.setProperty(SDKGlobalConfiguration.DISABLE_CERT_CHECKING_SYSTEM_PROPERTY, "true")

  val region: Regions              = Regions.AP_NORTHEAST_1
  val accessKeyId: String          = "AKIAIOSFODNN7EXAMPLE"
  val secretAccessKey: String      = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
  val hostPort: Int                = temporaryServerPort()
  val endpointOfLocalStack: String = s"http://$dockerHost:$hostPort"

  val controller: LocalStackController =
    new LocalStackController(
      dockerClient,
      envVars = Map(
        "DYNAMODB_SHARE_DB"  -> "1",
        "DYNAMODB_IN_MEMORY" -> "1"
      )
    )(
      services = Set(Service.DynamoDB, Service.DynamoDBStreams, Service.CloudWatch),
      edgeHostPort = hostPort,
      hostPorts = Map.empty,
      hostNameExternal = Some(dockerHost),
      defaultRegion = Some(region.getName)
    ) {
      override protected def newRemoveContainerCmd(): RemoveContainerCmd = {
        require(containerId.isDefined)
        dockerClient.removeContainerCmd(containerId.get).withForce(true)
      }
    }

  override protected val dockerControllers: Vector[DockerController] = Vector(controller)
  override protected val waitPredicatesSettings: Map[DockerController, WaitPredicateSetting] =
    Map(
      controller -> WaitPredicateSetting(Duration.Inf, WaitPredicates.forLogMessageExactly("Ready."))
    )

  var awsKinesisClient: AmazonKinesis                            = _
  var kinesisProducerConfiguration: KinesisProducerConfiguration = _

  val streamName: String = sys.env.getOrElse("STREAM_NAME", "kpl-flow-spec") + UUID.randomUUID().toString

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

  override def afterStartContainers(): Unit = {
    val credentialsProvider = new AWSStaticCredentialsProvider(new BasicAWSCredentials("x", "x"))
    val host                = "localhost"
    val kinesisPort         = hostPort
    val cloudwatchPort      = hostPort

    println(s"kinesis = $kinesisPort, cloudwatch = $cloudwatchPort")

    awsKinesisClient = AmazonKinesisClientBuilder
      .standard()
      .withCredentials(credentialsProvider)
      .withEndpointConfiguration(
        new EndpointConfiguration(s"https://$host:$kinesisPort", Regions.AP_NORTHEAST_1.getName)
      )
      .build()

    awsKinesisClient.createStream(streamName, 1)
    waitStreamToCreated(streamName)

    kinesisProducerConfiguration = new KinesisProducerConfiguration()
      .setCredentialsProvider(credentialsProvider)
      .setRegion(Regions.AP_NORTHEAST_1.getName)
      .setKinesisEndpoint(host)
      .setKinesisPort(kinesisPort.toLong)
      .setCloudwatchEndpoint(host)
      .setCloudwatchPort(cloudwatchPort.toLong)
      .setCredentialsRefreshDelay(100 * sys.env.getOrElse("TEST_TIME_FACTOR", "1").toInt)
      .setVerifyCertificate(false)
  }

  override def beforeStopContainers(): Unit = {
    awsKinesisClient.deleteStream(streamName)
  }

  "KPLFlow" - {
    "publisher" in {
      implicit val ec = system.dispatcher

      var result: UserRecordResult = null
      val partitionKey             = "123"
      val data                     = "XYZ"

      val kplFlowSettings = KPLFlowSettings.byNumberOfShards(1)
      val (sw, future) = Source
        .single(
          new UserRecord()
            .withStreamName(streamName)
            .withPartitionKey(partitionKey)
            .withData(ByteBuffer.wrap(data.getBytes()))
        )
        .viaMat(KillSwitches.single)(Keep.right)
        .viaMat(KPLFlow(streamName, kinesisProducerConfiguration, kplFlowSettings))(Keep.left)
        .toMat(Sink.foreach { msg => result = msg })(Keep.both)
        .run()

      TimeUnit.SECONDS.sleep(10 * sys.env.getOrElse("TEST_TIME_FACTOR", "1").toInt)

      sw.shutdown()
      future.futureValue
      result should not be null
      result.getShardId should not be null
      result.getSequenceNumber should not be null
      result.getAttempts.asScala.forall(_.isSuccessful) shouldBe true
    }
  }
}
