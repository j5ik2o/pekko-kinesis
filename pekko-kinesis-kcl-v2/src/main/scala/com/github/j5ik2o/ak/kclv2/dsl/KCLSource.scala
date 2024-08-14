package com.github.j5ik2o.ak.kclv2.dsl

import com.github.j5ik2o.ak.kclv2.stage.KCLSourceStage
import com.github.j5ik2o.ak.kclv2.stage.KCLSourceStage.{ CommittableRecord, ConfigsBuilderF, SchedulerF }
import org.apache.pekko.stream.scaladsl.Source
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.kinesis.coordinator.Scheduler

import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration.{ DurationInt, FiniteDuration }

object KCLSource {

  def apply(
      configsBuilderFactory: ConfigsBuilderF,
      schedulerFactory: SchedulerF,
      checkSchedulerPeriodicity: FiniteDuration = 1.seconds
  )(implicit ec: ExecutionContext): Source[CommittableRecord, Future[Scheduler]] = {
    Source.fromGraph(KCLSourceStage(configsBuilderFactory, schedulerFactory, checkSchedulerPeriodicity))
  }

  def from(
      streamName: String,
      applicationName: String,
      kinesisAsyncClient: KinesisAsyncClient,
      dynamoDbAsyncClient: DynamoDbAsyncClient,
      cloudWatchAsyncClient: CloudWatchAsyncClient,
      workerIdentifier: String,
      checkSchedulerPeriodicity: FiniteDuration = 1.seconds
  )(implicit ec: ExecutionContext): Source[CommittableRecord, Future[Scheduler]] = {
    Source.fromGraph(
      KCLSourceStage.from(
        streamName,
        applicationName,
        kinesisAsyncClient,
        dynamoDbAsyncClient,
        cloudWatchAsyncClient,
        workerIdentifier,
        checkSchedulerPeriodicity
      )
    )
  }

}
