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
package org.apache.spark.angel.ml.evaluation.evaluating

import org.apache.spark.angel.ml.evaluation.RegressionMetrics.RegressionPredictedResult
import org.apache.spark.angel.ml.evaluation.{RegressionMetrics, RegressionSummary}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}

class RegressionSummaryImpl(df: DataFrame, predictionCol: String, labelCol: String) extends RegressionSummary with Serializable {
  private lazy val data: RDD[RegressionPredictedResult] = df.select(predictionCol, labelCol).rdd.map {
    case Row(probability: Double, label: Double) =>
      RegressionPredictedResult(probability, label.toInt)
  }

  override val regMetrics: RegressionMetrics = data.aggregate(new RegressionMetrics)(
    seqOp = (metrics: RegressionMetrics, pres: RegressionPredictedResult) => metrics.add(pres),
    combOp = (metrics1: RegressionMetrics, metrics2: RegressionMetrics) => metrics1.merge(metrics2)
  )
}
