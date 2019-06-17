package com.tencent.angel.sona.core

import com.tencent.angel.ml.core.conf.SharedConf
import com.tencent.angel.ml.core.variable.VarState
import com.tencent.angel.sona.ml.AngeGraphModel
import org.apache.spark.TaskContext

import scala.collection.mutable

class GraphModelPool(sparkEnvContext: SparkEnvContext, numTask: Int) {
  @transient private lazy val modelQueue = new mutable.Queue[AngeGraphModel]()
  @transient private lazy val usedMap = new mutable.HashMap[Long, AngeGraphModel]()

  def borrowModel(conf: SharedConf): AngeGraphModel = this.synchronized {
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
}
