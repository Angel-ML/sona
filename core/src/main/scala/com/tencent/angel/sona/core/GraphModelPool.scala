package com.tencent.angel.sona.core

import com.tencent.angel.mlcore.conf.SharedConf
import com.tencent.angel.mlcore.variable.VarState
import com.tencent.angel.sona.ml.AngelGraphModel
import org.apache.spark.TaskContext

import scala.collection.mutable

class GraphModelPool(sparkEnvContext: SparkWorkerContext, numTask: Int) {
  @transient private lazy val modelQueue = new mutable.Queue[AngelGraphModel]()
  @transient private lazy val usedMap = new mutable.HashMap[Long, AngelGraphModel]()

  def borrowModel(conf: SharedConf): AngelGraphModel = this.synchronized {
    val partitionId = TaskContext.getPartitionId()

    if (usedMap.contains(partitionId)) {
      usedMap(partitionId)
    } else if (modelQueue.isEmpty) {
      val model = new AngelGraphModel(conf, numTask)
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

  def returnModel(model: AngelGraphModel): Unit = this.synchronized {
    val partitionId = TaskContext.getPartitionId()
    require(usedMap.contains(partitionId), "Error!")
    modelQueue.enqueue(model)
    usedMap.remove(partitionId)
  }
}
