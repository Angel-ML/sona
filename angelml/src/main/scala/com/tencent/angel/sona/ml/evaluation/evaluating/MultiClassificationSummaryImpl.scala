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

package com.tencent.angel.sona.ml.evaluation.evaluating
import com.tencent.angel.sona.ml.evaluation.{MultiClassMetrics, MultiClassificationSummary}
import com.tencent.angel.sona.ml.evaluation.MultiClassMetrics.MultiPredictedResult
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}

class MultiClassificationSummaryImpl(df: DataFrame, predictionCol: String, labelCol: String)
  extends MultiClassificationSummary with Serializable with Logging {

  private lazy val data: RDD[MultiPredictedResult] = df.select(predictionCol, labelCol).rdd.map {
    case Row(predictediction: Double, label: Double) =>
      MultiPredictedResult(predictediction.toInt, label.toInt)
  }

  lazy val multiMetrics: MultiClassMetrics = data.aggregate(new MultiClassMetrics)(
    seqOp = (metrics: MultiClassMetrics, pres: MultiPredictedResult) => metrics.add(pres),
    combOp = (metrics1: MultiClassMetrics, metrics2: MultiClassMetrics) => metrics1.merge(metrics2)
  )
}
