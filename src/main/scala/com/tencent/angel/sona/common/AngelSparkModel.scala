package com.tencent.angel.sona.common

import com.tencent.angel.client.AngelPSClient
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.param.Params
import com.tencent.angel.sona.common.measure.TrainingStat
import com.tencent.angel.sona.common.params.AngelGraphParams
import com.tencent.angel.sona.core.{AngeGraphModel, DriverContext, ExecutorContext, SparkEnvContext}

trait AngelSparkModel extends Params with AngelGraphParams {
  val angelModelName: String

  var numTask: Int = -1

  @transient var bcValue: Broadcast[ExecutorContext] = _

  @transient implicit val psClient: AngelPSClient = synchronized {
    DriverContext.get().getAngelClient
  }

  @transient lazy val sparkEnvContext: SparkEnvContext = synchronized {
    DriverContext.get().sparkEnvContext
  }

  @transient implicit lazy val dim: Long = getNumFeature

  @transient lazy val angelModel: AngeGraphModel = {
    require(numTask == -1 || numTask > 0, "Please set numTask before use angelModel")
    new AngeGraphModel(sharedConf, numTask)
  }

  @transient private var trainingSummary: Option[TrainingStat] = None

  def setSummary(summary: Option[TrainingStat]): this.type = {
    this.trainingSummary = summary
    this
  }

  def hasSummary: Boolean = trainingSummary.isDefined

  def summary: TrainingStat = trainingSummary.getOrElse {
    throw new Exception("No training summary available for this AngelClassifierModel")
  }

  def setNumTask(numTask: Int): this.type = {
    this.numTask = numTask
    psClient.setTaskNum(numTask)

    this
  }

  def setBCValue(bcValue: Broadcast[ExecutorContext]): this.type = {
    this.bcValue = bcValue

    this
  }

}
