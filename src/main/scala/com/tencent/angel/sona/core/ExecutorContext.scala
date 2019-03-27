package com.tencent.angel.sona.core

import com.tencent.angel.ml.core.conf.SharedConf
import com.tencent.angel.ml.core.variable.VarState
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging

import scala.collection.mutable

case class ExecutorContext(conf: SharedConf, numTask: Int,
                           partitionStat: Map[Int, Int],
                           sharedParams: Boolean = false)
  extends PSAgentContext(conf) with Serializable with Logging {

  @transient lazy val lower: Double = 1.0 / Math.log(Double.MaxValue)
  @transient lazy val upper: Double = Math.log(Double.MaxValue)

  @transient private lazy val modelQueue = new mutable.Queue[AngeGraphModel]()
  @transient private lazy val usedMap = new mutable.HashMap[Long, AngeGraphModel]()

  @transient override lazy val sparkEnvContext: SparkEnvContext = SparkEnvContext(null)

  def borrowModel: AngeGraphModel = this.synchronized {
    createAndInitPSAgent

    val partitionId = TaskContext.getPartitionId()

    if (usedMap.contains(partitionId)) {
      usedMap(partitionId)
    } else if (modelQueue.isEmpty) {
      val model = new AngeGraphModel(conf, numTask)
      model.buildNetwork()
      model.createMatrices(sparkEnvContext)
      model.setState(VarState.Initialized)

      usedMap.put(partitionId, model)
      model
    } else {
      val model = modelQueue.dequeue()

      usedMap.put(partitionId, model)
      model
    }
  }

  def returnModel(model: AngeGraphModel): Unit = this.synchronized {
    val partitionId = TaskContext.getPartitionId()
    require(usedMap.contains(partitionId), "Error!")
    modelQueue.enqueue(model)
    usedMap.remove(partitionId)
  }

  def getSamplesInPartition: Int = {
    partitionStat(TaskContext.getPartitionId())
  }
}
