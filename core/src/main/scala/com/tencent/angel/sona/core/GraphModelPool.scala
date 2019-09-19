/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017-2018 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */
package com.tencent.angel.sona.core

import com.tencent.angel.mlcore.conf.SharedConf
import com.tencent.angel.mlcore.variable.VarState
import org.apache.spark.TaskContext

import scala.collection.mutable

class GraphModelPool(sparkEnvContext: SparkWorkerContext, numTask: Int) extends Serializable {
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
