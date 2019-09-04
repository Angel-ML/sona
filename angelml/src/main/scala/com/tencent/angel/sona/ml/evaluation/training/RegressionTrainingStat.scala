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

package com.tencent.angel.sona.ml.evaluation.training
import com.tencent.angel.mlcore.PredictResult
import com.tencent.angel.sona.ml.evaluation.RegressionMetrics.RegressionPredictedResult
import com.tencent.angel.sona.ml.evaluation.{RegressionMetrics, RegressionSummary, TrainingStat}

class RegressionTrainingStat extends TrainingStat with Serializable {
  lazy val stat = new RegressionMetrics

  def getSummary: RegressionSummary = {
    new RegressionSummary{
      override val regMetrics: RegressionMetrics = stat
    }
  }

  override def printString(): String = {
    val summary = getSummary
    val insert = f"MSE=${summary.mse}%.3f, RMSE=${summary.rmse}%.3f, R2=${summary.r2}%.3f, " +
      f"explainedVariance=${summary.explainedVariance}%.3f"

    printString(insert)
  }

  override def add(pres: PredictResult): this.type = {
    stat.add(RegressionPredictedResult(pres.pred, pres.trueLabel))
    this
  }

  override protected def mergePart(other: TrainingStat): Unit = {
    val o = other.asInstanceOf[RegressionTrainingStat]

    stat.merge(o.stat)

    super.mergePart(other)
  }

  override def clearStat(): this.type = {
    stat.clear()

    this
  }
}
