/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tencent.angel.sona.ml.evaluation

import com.tencent.angel.sona.ml.evaluation.evaluating.RegressionSummaryImpl
import com.tencent.angel.sona.ml.param.{Param, ParamMap, ParamValidators}
import com.tencent.angel.sona.ml.param.shared.{HasLabelCol, HasPredictionCol}
import com.tencent.angel.sona.ml.util._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types.{DoubleType, FloatType}
import org.apache.spark.sql.util.SchemaUtils


/**
 * :: Experimental ::
 * Evaluator for regression, which expects two input columns: prediction and label.
 */
final class RegressionEvaluator(override val uid: String)
  extends Evaluator with HasPredictionCol with HasLabelCol with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("regEval"))

  /**
   * Param for metric name in evaluation. Supports:
   *  - `"rmse"` (default): root mean squared error
   *  - `"mse"`: mean squared error
   *  - `"r2"`: R^2^ metric
   *  - `"mae"`: mean absolute error
   *
   * @group param
   */
  val metricName: Param[String] = {
    val allowedParams = ParamValidators.inArray(Array("mse", "rmse", "r2", "mae"))
    new Param(this, "metricName", "metric name in evaluation (mse|rmse|r2|mae)", allowedParams)
  }

  /** @group getParam */
  def getMetricName: String = $(metricName)

  /** @group setParam */
  def setMetricName(value: String): this.type = set(metricName, value)

  /** @group setParam */
  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  /** @group setParam */
  def setLabelCol(value: String): this.type = set(labelCol, value)

  setDefault(metricName -> "rmse")

  override def evaluate(dataset: Dataset[_]): Double = {
    val schema = dataset.schema
    SchemaUtils.checkColumnTypes(schema, $(predictionCol), Seq(DoubleType, FloatType))
    SchemaUtils.checkNumericType(schema, $(labelCol))

    val summary = new RegressionSummaryImpl(dataset.toDF(), $(predictionCol), $(labelCol))
    val metrics = summary.regMetrics

    val metric = $(metricName) match {
      case "rmse" => summary.rmse
      case "mse" => summary.mse
      case "r2" => summary.r2
      case "mae" => summary.absDiff
    }

    metric
  }

  override def isLargerBetter: Boolean = $(metricName) match {
    case "rmse" => false
    case "mse" => false
    case "r2" => true
    case "mae" => false
  }

  override def copy(extra: ParamMap): RegressionEvaluator = defaultCopy(extra)
}


object RegressionEvaluator extends DefaultParamsReadable[RegressionEvaluator] {
  override def load(path: String): RegressionEvaluator = super.load(path)
}
