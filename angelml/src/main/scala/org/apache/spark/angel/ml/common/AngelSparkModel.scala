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
package org.apache.spark.angel.ml.common

import com.tencent.angel.client.AngelPSClient
import com.tencent.angel.mlcore.conf.SharedConf
import com.tencent.angel.sona.core.{DriverContext, ExecutorContext, SparkMasterContext}
import com.tencent.angel.sona.ml.AngelGraphModel
import org.apache.spark.angel.ml.evaluation.TrainingStat
import org.apache.spark.angel.ml.param.{AngelGraphParams, Params}
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

  @transient lazy val angelModel: AngelGraphModel = {
    require(numTask == -1 || numTask > 0, "Please set numTask before use angelModel")
    new AngelGraphModel(sharedConf, numTask)
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
