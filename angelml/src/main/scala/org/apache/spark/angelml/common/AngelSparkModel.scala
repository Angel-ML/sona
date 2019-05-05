package org.apache.spark.angelml.common

import com.tencent.angel.client.AngelPSClient
import com.tencent.angel.sona.core.{DriverContext, ExecutorContext, SparkEnvContext}
import com.tencent.angel.sona.ml.AngeGraphModel
import org.apache.spark.angelml.evaluation.TrainingStat
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.angelml.param.{AngelGraphParams, Params}
import org.apache.spark.angelml.param.Params

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
