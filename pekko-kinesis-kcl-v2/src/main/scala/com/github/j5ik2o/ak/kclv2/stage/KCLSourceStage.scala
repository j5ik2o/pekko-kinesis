package com.github.j5ik2o.ak.kclv2.stage

import com.amazonaws.services.schemaregistry.common.Schema
import com.github.j5ik2o.ak.kclv2.stage.KCLSourceStage.{
  CommittableRecord,
  ConfigsBuilderF,
  RecordSet,
  SchedulerF,
  TimerKey
}
import org.apache.pekko.stream.stage._
import org.apache.pekko.stream.{ Attributes, Outlet, SourceShape }
import org.slf4j.LoggerFactory
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.awssdk.services.kinesis.model.{ ChildShard, EncryptionType }
import software.amazon.kinesis.checkpoint.CheckpointConfig
import software.amazon.kinesis.common.ConfigsBuilder
import software.amazon.kinesis.coordinator.{ CoordinatorConfig, Scheduler }
import software.amazon.kinesis.leases.LeaseManagementConfig
import software.amazon.kinesis.lifecycle.LifecycleConfig
import software.amazon.kinesis.lifecycle.events.{ ProcessRecordsInput, _ }
import software.amazon.kinesis.metrics.MetricsConfig
import software.amazon.kinesis.processor.{
  ProcessorConfig,
  RecordProcessorCheckpointer,
  ShardRecordProcessor,
  ShardRecordProcessorFactory
}
import software.amazon.kinesis.retrieval.{ KinesisClientRecord, RetrievalConfig }

import java.nio.ByteBuffer
import java.time.Instant
import scala.collection.immutable.Queue
import scala.collection.mutable
import scala.concurrent.duration.{ DurationInt, FiniteDuration }
import scala.concurrent.{ ExecutionContext, Future, Promise }
import scala.jdk.CollectionConverters._
import scala.util.{ Failure, Success, Try }
import scala.util.control.{ NoStackTrace, NonFatal }

sealed trait KinesisSchedulerSourceError extends NoStackTrace

case object SchedulerUnexpectedShutdown extends KinesisSchedulerSourceError

object KCLSourceStage {
  final case class CommittableRecord(
      kinesisClientRecord: KinesisClientRecord,
      checkPointer: RecordProcessorCheckpointer
  ) {
    def sequenceNumber: String               = kinesisClientRecord.sequenceNumber()
    def approximateArrivalTimestamp: Instant = kinesisClientRecord.approximateArrivalTimestamp()
    def data: ByteBuffer                     = kinesisClientRecord.data()
    def partitionKey: String                 = kinesisClientRecord.partitionKey()
    def encryptionType: EncryptionType       = kinesisClientRecord.encryptionType()
    def subSequenceNumber: Long              = kinesisClientRecord.subSequenceNumber()
    def explicitHashKey: String              = kinesisClientRecord.explicitHashKey()
    def aggregated: Boolean                  = kinesisClientRecord.aggregated()
    def schema: Schema                       = kinesisClientRecord.schema()

    def checkPointAsync()(implicit ec: ExecutionContext): Future[Unit] = Future { checkPointSync().get }

    def checkPointSync(): Try[Unit] = Try {
      checkPointer.checkpoint(sequenceNumber, subSequenceNumber)
    }
  }

  final case class RecordSet(
      processRecordsInput: ProcessRecordsInput
  ) {
    def cacheEntryTime: Instant = processRecordsInput.cacheEntryTime()
    def cacheExitTime: Instant  = processRecordsInput.cacheExitTime()
    def isAtShardEnd: Boolean   = processRecordsInput.isAtShardEnd
    def records: Vector[CommittableRecord] = processRecordsInput
      .records().asScala.map { record => CommittableRecord(record, processRecordsInput.checkpointer()) }.toVector
    def checkPointer: RecordProcessorCheckpointer = processRecordsInput.checkpointer()
    def millisBehindLatest: Long                  = processRecordsInput.millisBehindLatest()
    def childShards: Vector[ChildShard]           = processRecordsInput.childShards().asScala.toVector
  }

  private final class RecordProcessorFactory(
      onInitializeCallback: AsyncCallback[InitializationInput],
      onRecordsCallback: AsyncCallback[ProcessRecordsInput],
      onLeaseLostCallback: AsyncCallback[(String, LeaseLostInput)],
      onShardEndedCallback: AsyncCallback[(String, ShardEndedInput, Try[Unit])],
      onShutdownRequestedCallback: AsyncCallback[(String, ShutdownRequestedInput)]
  ) extends ShardRecordProcessorFactory {
    private[this] val logger = LoggerFactory.getLogger(getClass)
    logger.debug("RecordProcessorFactory: start")
    override def shardRecordProcessor(): ShardRecordProcessor = {
      logger.debug("shardRecordProcessor: start")
      new RecordProcessor(
        onInitializeCallback,
        onRecordsCallback,
        onLeaseLostCallback,
        onShardEndedCallback,
        onShutdownRequestedCallback
      )
    }
  }

  private final class RecordProcessor(
      onInitializeCallback: AsyncCallback[InitializationInput],
      onRecordsCallback: AsyncCallback[ProcessRecordsInput],
      onLeaseLostCallback: AsyncCallback[(String, LeaseLostInput)],
      onShardEndedCallback: AsyncCallback[(String, ShardEndedInput, Try[Unit])],
      onShutdownRequestedCallback: AsyncCallback[(String, ShutdownRequestedInput)]
  ) extends ShardRecordProcessor {
    private[this] val logger           = LoggerFactory.getLogger(getClass)
    private[this] var _shardId: String = _

    override def initialize(initializationInput: InitializationInput): Unit = {
      logger.debug(s"initialize:initializationInput = ${initializationInput.toString}")
      _shardId = initializationInput.shardId
      onInitializeCallback.invoke(initializationInput)
    }

    override def processRecords(processRecordsInput: ProcessRecordsInput): Unit = {
      logger.debug(s"processRecords:processRecordsInput = ${processRecordsInput.toString}")
      onRecordsCallback.invoke(processRecordsInput)
    }

    override def leaseLost(leaseLostInput: LeaseLostInput): Unit = {
      logger.debug(s"leaseLost:leaseLostInput = ${leaseLostInput.toString}")
      onLeaseLostCallback.invoke((_shardId, leaseLostInput))
    }

    override def shardEnded(shardEndedInput: ShardEndedInput): Unit = {
      logger.debug(s"shardEnded:shardEndedInput = ${shardEndedInput.toString}")
      try {
        shardEndedInput.checkpointer().checkpoint()
        onShardEndedCallback.invoke((_shardId, shardEndedInput, Success(())))
      } catch {
        case NonFatal(ex) =>
          logger.error(s"shardEnded: error occurred: ${ex.getMessage}")
          onShardEndedCallback.invoke((_shardId, shardEndedInput, Failure(ex)))
      }
    }

    override def shutdownRequested(shutdownRequestedInput: ShutdownRequestedInput): Unit = {
      logger.debug(s"shutdownRequested:shutdownRequestedInput = ${shutdownRequestedInput.toString}")
      try {
        shutdownRequestedInput.checkpointer().checkpoint()
        onShutdownRequestedCallback.invoke((_shardId, shutdownRequestedInput))
      } catch {
        case NonFatal(ex) =>
          logger.error(s"shutdownRequested: error occurred: ${ex.getMessage}")
      }
    }
  }

  type ConfigsBuilderF = (
      AsyncCallback[InitializationInput],
      AsyncCallback[ProcessRecordsInput],
      AsyncCallback[(String, LeaseLostInput)],
      AsyncCallback[(String, ShardEndedInput, Try[Unit])],
      AsyncCallback[(String, ShutdownRequestedInput)]
  ) => ConfigsBuilder

  def configsBuilderFactory(
      streamName: String,
      applicationName: String,
      kinesisAsyncClient: KinesisAsyncClient,
      dynamoDbAsyncClient: DynamoDbAsyncClient,
      cloudWatchAsyncClient: CloudWatchAsyncClient,
      workerIdentifier: String
  ): ConfigsBuilderF = {
    (onInitializeCallback, onRecordsCallback, onLeaseLostCallback, onShardEndedCallback, onShutdownRequestedCallback) =>
      new ConfigsBuilder(
        streamName,
        applicationName,
        kinesisAsyncClient,
        dynamoDbAsyncClient,
        cloudWatchAsyncClient,
        workerIdentifier,
        new RecordProcessorFactory(
          onInitializeCallback,
          onRecordsCallback,
          onLeaseLostCallback,
          onShardEndedCallback,
          onShutdownRequestedCallback
        )
      )
  }

  type SchedulerF = ConfigsBuilder => Scheduler

  def schedulerFactory(
      checkpointConfigF: Option[CheckpointConfig => CheckpointConfig] = None,
      coordinatorConfigF: Option[CoordinatorConfig => CoordinatorConfig] = None,
      leaseManagementConfigF: Option[LeaseManagementConfig => LeaseManagementConfig] = None,
      lifecycleConfigF: Option[LifecycleConfig => LifecycleConfig] = None,
      metricsConfigF: Option[MetricsConfig => MetricsConfig] = None,
      processorConfigF: Option[ProcessorConfig => ProcessorConfig] = None,
      retrievalConfigF: Option[RetrievalConfig => RetrievalConfig] = None
  ): SchedulerF = { configsBuilder =>
    new Scheduler(
      checkpointConfigF.fold(configsBuilder.checkpointConfig())(_(configsBuilder.checkpointConfig())),
      coordinatorConfigF.fold(configsBuilder.coordinatorConfig())(_(configsBuilder.coordinatorConfig())),
      leaseManagementConfigF.fold(configsBuilder.leaseManagementConfig())(_(configsBuilder.leaseManagementConfig())),
      lifecycleConfigF.fold(configsBuilder.lifecycleConfig())(_(configsBuilder.lifecycleConfig())),
      metricsConfigF.fold(configsBuilder.metricsConfig())(_(configsBuilder.metricsConfig())),
      processorConfigF.fold(configsBuilder.processorConfig())(_(configsBuilder.processorConfig())),
      retrievalConfigF.fold(configsBuilder.retrievalConfig())(_(configsBuilder.retrievalConfig()))
    )
  }

  final val TimerKey = "check-scheduler-shutdown"

  def from(
      streamName: String,
      applicationName: String,
      kinesisAsyncClient: KinesisAsyncClient,
      dynamoDbAsyncClient: DynamoDbAsyncClient,
      cloudWatchAsyncClient: CloudWatchAsyncClient,
      workerIdentifier: String,
      checkSchedulerPeriodicity: FiniteDuration = 1.seconds
  )(implicit ec: ExecutionContext): KCLSourceStage = {
    val configsBuilderFactory = KCLSourceStage.configsBuilderFactory(
      streamName,
      applicationName,
      kinesisAsyncClient,
      dynamoDbAsyncClient,
      cloudWatchAsyncClient,
      workerIdentifier
    )
    val schedulerFactory = KCLSourceStage.schedulerFactory()
    new KCLSourceStage(configsBuilderFactory, schedulerFactory, checkSchedulerPeriodicity)
  }

  def apply(
      configsBuilderFactory: ConfigsBuilderF,
      schedulerFactory: SchedulerF,
      checkSchedulerPeriodicity: FiniteDuration = 1.seconds
  )(implicit ec: ExecutionContext): KCLSourceStage =
    new KCLSourceStage(configsBuilderFactory, schedulerFactory, checkSchedulerPeriodicity)
}

final class KCLSourceStage(
    configsBuilderFactory: ConfigsBuilderF,
    schedulerFactory: SchedulerF,
    checkSchedulerPeriodicity: FiniteDuration = 1.seconds
)(implicit ec: ExecutionContext)
    extends GraphStageWithMaterializedValue[SourceShape[CommittableRecord], Future[Scheduler]] {
  private val out: Outlet[CommittableRecord]         = Outlet("KCLv2Source.out")
  override def shape: SourceShape[CommittableRecord] = SourceShape(out)

  override def createLogicAndMaterializedValue(
      inheritedAttributes: Attributes
  ): (GraphStageLogic, Future[Scheduler]) = {
    val schedulerPromise = Promise[Scheduler]()
    val logic = new TimerGraphStageLogic(shape) with StageLogging {
      private var scheduler: Scheduler              = _
      private var buffer: Queue[RecordSet]          = Queue.empty[RecordSet]
      private val shardIds: mutable.HashSet[String] = mutable.HashSet.empty[String]

      private val onInitializationCallback: AsyncCallback[InitializationInput] = getAsyncCallback {
        initializationInput =>
          log.debug(
            s"onInitializeCallback: initializationInput = ${initializationInput.toString}"
          )
          shardIds += initializationInput.shardId
      }

      private val onProcessRecordsCallback: AsyncCallback[ProcessRecordsInput] = getAsyncCallback {
        processRecordsInput =>
          log.debug(s"onRecordSetCallback: processRecordsInput = ${processRecordsInput.toString}")
          val recordSet = RecordSet(processRecordsInput)
          buffer = buffer.enqueue(recordSet)
          tryToProduce()
      }

      private val onLeaseLostCallback: AsyncCallback[(String, LeaseLostInput)] = getAsyncCallback {
        case (shardId, leaseLostInput) =>
          log.debug(s"onLeaseLostCallback: (shardId, leaseLostInput) = ($shardId, ${leaseLostInput.toString})")
          shardIds -= shardId
      }

      private val onShardEndedCallback: AsyncCallback[(String, ShardEndedInput, Try[Unit])] = getAsyncCallback {
        case (shardId, shardEndedInput, _) =>
          log.debug(s"onShardEndedCallback: (shardId, shardEndedInput) = ($shardId, ${shardEndedInput.toString})")
          shardIds -= shardId
      }

      private val onShutdownRequestedCallback: AsyncCallback[(String, ShutdownRequestedInput)] = getAsyncCallback {
        case (shardId, shutdownRequestedInput) =>
          log.debug(
            s"onShutdownRequestedCallback: (shardId, shutdownRequestedInput) = ($shardId, ${shutdownRequestedInput.toString})"
          )
          shardIds -= shardId
      }

      private def tryToProduce(): Unit = {
        if (buffer.nonEmpty && isAvailable(out)) {
          val (head, tail) = buffer.dequeue
          buffer = tail
          val records = head.records
          emitMultiple(out, records)
          log.debug(s"tryToProduce: emit: $records")
        }
      }

      override def preStart(): Unit = {
        val configsBuilder = configsBuilderFactory(
          onInitializationCallback,
          onProcessRecordsCallback,
          onLeaseLostCallback,
          onShardEndedCallback,
          onShutdownRequestedCallback
        )
        val scheduler = schedulerFactory(configsBuilder)
        log.debug(s"Created Scheduler instance: scheduler = ${scheduler.applicationName}")
        scheduleAtFixedRate(TimerKey, checkSchedulerPeriodicity, checkSchedulerPeriodicity)
        scheduler.run()
        this.scheduler = scheduler
        val thread = new Thread(scheduler)
        thread.setDaemon(true)
        thread.start()
        schedulerPromise.success(scheduler)
      }

      override def postStop(): Unit = {
        buffer = Queue.empty[RecordSet]
        scheduler.gracefulShutdownFuture().get()
        log.info(s"Shut down Scheduler instance: scheduler = ${scheduler.applicationName}")
      }

      override protected def onTimer(timerKey: Any): Unit = {
        timerKey match {
          case TimerKey =>
            if (scheduler.hasGracefulShutdownStarted && isAvailable(out)) {
              log.warning(s"onTimer($timerKey): failStage: scheduler unexpected shutdown")
              failStage(SchedulerUnexpectedShutdown)
            }
          case _ =>
            throw new IllegalStateException(s"Invalid timerKey: timerKey = ${timerKey.toString}")
        }
      }

      setHandler(
        out,
        new OutHandler {
          override def onPull(): Unit = tryToProduce()
        }
      )

    }

    (logic, schedulerPromise.future)
  }

}
