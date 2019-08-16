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

import org.apache.spark.angel.ml.evaluation.BinaryClassMetrics.BinaryPredictedResult
import org.apache.spark.angel.ml.evaluation.{BinaryClassMetrics, BinaryClassificationSummary}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}


class BinaryClassificationSummaryImpl(df: DataFrame,
                                      probabilityCol: String,
                                      labelCol: String)
  extends BinaryClassificationSummary with Serializable with Logging {

  private lazy val data: RDD[BinaryPredictedResult] = df.select(probabilityCol, labelCol).rdd.map {
    case Row(probability: Double, label: Double) =>
      BinaryPredictedResult(probability, label.toInt)
  }

  lazy val binaryMetrics: BinaryClassMetrics = data.aggregate(new BinaryClassMetrics)(
    seqOp = (metrics: BinaryClassMetrics, pres: BinaryPredictedResult) => metrics.add(pres),
    combOp = (metrics1: BinaryClassMetrics, metrics2: BinaryClassMetrics) => metrics1.merge(metrics2)
  )

  protected lazy val (tp: Double, fp: Double, fn: Double, tn: Double) = (
    binaryMetrics.getTP, binaryMetrics.getFP, binaryMetrics.getFN, binaryMetrics.getTN)
}


