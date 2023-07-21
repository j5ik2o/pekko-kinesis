package com.github.j5ik2o.ak.kcl.dyanmodb.streams

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.KillSwitches
import org.apache.pekko.stream.scaladsl.{ Keep, Sink }
import org.apache.pekko.testkit.TestKit
import com.amazonaws.SDKGlobalConfiguration
import com.amazonaws.auth.{ AWSCredentialsProvider, AWSStaticCredentialsProvider, BasicAWSCredentials }
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.regions.Regions
import com.amazonaws.services.cloudwatch.{ AmazonCloudWatch, AmazonCloudWatchClient }
import com.amazonaws.services.dynamodbv2.model._
import com.amazonaws.services.dynamodbv2.streamsadapter.AmazonDynamoDBStreamsAdapterClient
import com.amazonaws.services.dynamodbv2.streamsadapter.model.RecordAdapter
import com.amazonaws.services.dynamodbv2.{
  AmazonDynamoDB,
  AmazonDynamoDBClient,
  AmazonDynamoDBStreams,
  AmazonDynamoDBStreamsClient
}
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import com.github.dockerjava.api.command.RemoveContainerCmd
import com.github.j5ik2o.ak.kcl.dsl.KCLFlow
import com.github.j5ik2o.ak.kcl.util.KCLConfiguration
import com.github.j5ik2o.dockerController.{ DockerController, DockerControllerSpecSupport, WaitPredicates }
import com.github.j5ik2o.dockerController.localstack.{ LocalStackController, Service }
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.concurrent.{ Eventually, ScalaFutures }
import org.scalatest.freespec.AnyFreeSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{ Millis, Seconds, Span }

import java.net.InetAddress
import java.util
import java.util.UUID
import java.util.concurrent.Executors
import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration

class KCLSourceOnDynamoDBStreamsSpec
    extends TestKit(ActorSystem("KCLSourceInDynamoDBStreamsSpec"))
    with AnyFreeSpecLike
    with Matchers
    with ScalaFutures
    with DockerControllerSpecSupport
    with Eventually {
  System.setProperty(SDKGlobalConfiguration.AWS_CBOR_DISABLE_SYSTEM_PROPERTY, "true")

  val testTimeFactor: Int = sys.env.getOrElse("TEST_TIME_FACTOR", "1").toInt
  logger.debug(s"testTimeFactor = $testTimeFactor")

  val region: Regions              = Regions.AP_NORTHEAST_1
  val accessKeyId: String          = "AKIAIOSFODNN7EXAMPLE"
  val secretAccessKey: String      = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
  val hostPort: Int                = temporaryServerPort()
  val endpointOfLocalStack: String = s"http://$dockerHost:$hostPort"

  implicit val defaultPatience: PatienceConfig =
    PatienceConfig(timeout = Span(60 * testTimeFactor, Seconds), interval = Span(500 * testTimeFactor, Millis))

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

  val applicationName: String                = "kcl-source-spec"
  val workerId: String                       = InetAddress.getLocalHost.getCanonicalHostName + ":" + UUID.randomUUID()
  val tableName: String                      = "test-" + UUID.randomUUID().toString
  var dynamoDBStreams: AmazonDynamoDBStreams = _
  var awsDynamoDB: AmazonDynamoDB            = _
  var awsCloudWatch: AmazonCloudWatch        = _

  override def afterStartContainers(): Unit = {
    val credentialsProvider = new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKeyId, secretAccessKey))
    val dynamoDbEndpointConfiguration   = new EndpointConfiguration(endpointOfLocalStack, region.getName)
    val cloudwatchEndpointConfiguration = new EndpointConfiguration(endpointOfLocalStack, region.getName)

    awsDynamoDB = AmazonDynamoDBClient
      .builder()
      .withCredentials(credentialsProvider)
      .withEndpointConfiguration(dynamoDbEndpointConfiguration)
      .build()

    dynamoDBStreams = AmazonDynamoDBStreamsClient
      .builder()
      .withCredentials(credentialsProvider)
      .withEndpointConfiguration(dynamoDbEndpointConfiguration)
      .build()

    awsCloudWatch = AmazonCloudWatchClient
      .builder()
      .withCredentials(credentialsProvider)
      .withEndpointConfiguration(cloudwatchEndpointConfiguration)
      .build()

  }

  import system.dispatcher

  "KCLSourceSpec" - {
    "dynamodb streams" in {
      val attributeDefinitions = new util.ArrayList[AttributeDefinition]
      attributeDefinitions.add(new AttributeDefinition().withAttributeName("Id").withAttributeType("N"))

      val keySchema = new util.ArrayList[KeySchemaElement]
      keySchema.add(new KeySchemaElement().withAttributeName("Id").withKeyType(KeyType.HASH))

      val request = new CreateTableRequest()
        .withTableName(tableName)
        .withKeySchema(keySchema)
        .withAttributeDefinitions(attributeDefinitions)
        .withProvisionedThroughput(
          new ProvisionedThroughput()
            .withReadCapacityUnits(10L)
            .withWriteCapacityUnits(10L)
        ).withStreamSpecification(
          new StreamSpecification()
            .withStreamViewType(StreamViewType.NEW_IMAGE)
            .withStreamEnabled(true)
        )
      val table = awsDynamoDB.createTable(request)

      while (!awsDynamoDB.listTables(1).getTableNames.asScala.contains(tableName)) {
        println("waiting for create table...")
        Thread.sleep(1000)
      }

      Thread.sleep(1000)

      val streamArn = table.getTableDescription.getLatestStreamArn
      val credentialsProvider: AWSCredentialsProvider =
        new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKeyId, secretAccessKey))

      val kinesisClientLibConfiguration = KCLConfiguration.fromConfig(
        system.settings.config,
        applicationName,
        UUID.randomUUID(),
        streamArn,
        credentialsProvider,
        credentialsProvider,
        credentialsProvider,
        configOverrides = Some(
          KCLConfiguration.ConfigOverrides(positionInStreamOpt = Some(InitialPositionInStream.TRIM_HORIZON))
        )
      )

      val adapterClient: AmazonDynamoDBStreamsAdapterClient = new AmazonDynamoDBStreamsAdapterClient(dynamoDBStreams)

      val executorService = Executors.newCachedThreadPool()
      var result: String  = null
      val (sw, future) =
        KCLSourceOnDynamoDBStreams
          .withoutCheckpoint(
            kinesisClientLibConfiguration = kinesisClientLibConfiguration,
            amazonDynamoDBStreamsAdapterClient = adapterClient,
            amazonDynamoDB = awsDynamoDB,
            amazonCloudWatchClientOpt = Some(awsCloudWatch),
            metricsFactoryOpt = None,
            execService = executorService
          )
          .viaMat(KillSwitches.single)(Keep.right)
          .map { msg =>
            val recordAdaptor = msg.record
              .asInstanceOf[RecordAdapter]
            val streamRecord = recordAdaptor.getInternalObject.getDynamodb
            val newImage     = streamRecord.getNewImage.asScala
            val id           = newImage("Id").getN
            val message      = newImage("Value").getS
            println(s"id = $id, message = $message")
            result = message
            msg
          }
          .via(KCLFlow.ofCheckpoint())
          .toMat(Sink.ignore)(Keep.both)
          .run()

      Thread.sleep(1000)

      val text = "abc"
      awsDynamoDB.putItem(
        new PutItemRequest()
          .withTableName(tableName)
          .withItem(Map("Id" -> new AttributeValue().withN("1"), "Value" -> new AttributeValue().withS(text)).asJava)
      )

      eventually(Timeout(Span.Max)) {
        assert(result != null && result == text)
      }

      sw.shutdown()
      future.futureValue
    }
  }
}
