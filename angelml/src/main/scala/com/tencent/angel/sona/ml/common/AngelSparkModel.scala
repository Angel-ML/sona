package com.tencent.angel.sona.ml.common

import com.tencent.angel.client.AngelPSClient
import com.tencent.angel.mlcore.conf.SharedConf
import com.tencent.angel.sona.core.{DriverContext, ExecutorContext, SparkMasterContext}
import com.tencent.angel.sona.ml.AngeGraphModel
import com.tencent.angel.sona.ml.evaluation.TrainingStat
import com.tencent.angel.sona.ml.param.{AngelGraphParams, Params}
import org.apache.spark.broadcast.Broadcast

trait AngelSparkModel extends Params with AngelGraphParams {
  val angelModelName: String

  var numTask: Int = -1

  @transient var bcValue: Broadcast[ExecutorContext] = _
  @transient var bcConf: Broadcast[SharedConf] = _

  @transient implicit val psClient: AngelPSClient = synchronized {
    DriverContext.get().getAngelClient
  }

  @transient lazy val sparkEnvContext: SparkMasterContext = synchronized {
    DriverContext.get().sparkMasterContext
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
