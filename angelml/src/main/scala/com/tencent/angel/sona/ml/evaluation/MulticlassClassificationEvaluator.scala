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

import com.tencent.angel.sona.ml.evaluation.evaluating.MultiClassificationSummaryImpl
import com.tencent.angel.sona.ml.param.{Param, ParamMap, ParamValidators}
import com.tencent.angel.sona.ml.param.shared.{HasLabelCol, HasPredictionCol}
import com.tencent.angel.sona.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.util.SchemaUtils
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types.DoubleType


/**
 * :: Experimental ::
 * Evaluator for multiclass classification, which expects two input columns: prediction and label.
 */

class MulticlassClassificationEvaluator(override val uid: String)
  extends Evaluator with HasPredictionCol with HasLabelCol with DefaultParamsWritable {


  def this() = this(Identifiable.randomUID("mcEval"))

  /**
   * param for metric name in evaluation (supports `"f1"` (default), `"weightedPrecision"`,
   * `"weightedRecall"`, `"accuracy"`)
   * @group param
   */

  val metricName: Param[String] = {
    val allowedParams = ParamValidators.inArray(Array("f1", "weightedPrecision",
      "weightedRecall", "accuracy"))
    new Param(this, "metricName", "metric name in evaluation " +
      "(f1|weightedPrecision|weightedRecall|accuracy)", allowedParams)
  }

  /** @group getParam */

  def getMetricName: String = $(metricName)

  /** @group setParam */

  def setMetricName(value: String): this.type = set(metricName, value)

  /** @group setParam */

  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  /** @group setParam */

  def setLabelCol(value: String): this.type = set(labelCol, value)

  setDefault(metricName -> "f1")


  override def evaluate(dataset: Dataset[_]): Double = {
    val schema = dataset.schema
    SchemaUtils.checkColumnType(schema, $(predictionCol), DoubleType)
    SchemaUtils.checkNumericType(schema, $(labelCol))

    val summary = new MultiClassificationSummaryImpl(dataset.toDF(), $(predictionCol), $(labelCol))

    val metrics = summary.multiMetrics
    val metric = $(metricName) match {
      case "f1" => summary.fMeasure(1.0)(0)
      case "weightedPrecision" => summary.precision(0)
      case "weightedRecall" => summary.recall(0)
      case "accuracy" => summary.accuracy
    }

    metric
  }


  override def isLargerBetter: Boolean = true


  override def copy(extra: ParamMap): MulticlassClassificationEvaluator = defaultCopy(extra)
}


object MulticlassClassificationEvaluator
  extends DefaultParamsReadable[MulticlassClassificationEvaluator] {


  override def load(path: String): MulticlassClassificationEvaluator = super.load(path)
}
