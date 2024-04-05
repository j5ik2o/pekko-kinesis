package com.github.j5ik2o.ak.kclv2.dsl

import com.github.j5ik2o.ak.kclv2.stage.KCLSourceStage.CommittableRecord
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Flow

import scala.concurrent.ExecutionContext

object KCLFlow {
  def ofCheckpoint()(implicit ec: ExecutionContext): Flow[CommittableRecord, CommittableRecord, NotUsed] =
    Flow[CommittableRecord]
      .mapAsync(1) { v =>
        v.checkPointAsync().map{ _ => v }
      }
}
