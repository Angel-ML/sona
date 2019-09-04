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

import com.tencent.angel.sona.ml.evaluation.evaluating.BinaryClassificationSummaryImpl
import org.apache.spark.linalg.VectorUDT
import com.tencent.angel.sona.ml.param.{Param, ParamMap, ParamValidators}
import com.tencent.angel.sona.ml.param.shared.{HasLabelCol, HasRawPredictionCol}
import com.tencent.angel.sona.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.util.SchemaUtils
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types.DoubleType


/**
 * :: Experimental ::
 * Evaluator for binary classification, which expects two input columns: rawPrediction and label.
 * The rawPrediction column can be of type double (binary 0/1 prediction, or probability of label 1)
 * or of type vector (length-2 vector of raw predictions, scores, or label probabilities).
 */

class BinaryClassificationEvaluator(override val uid: String)
  extends Evaluator with HasRawPredictionCol with HasLabelCol with DefaultParamsWritable {


  def this() = this(Identifiable.randomUID("binEval"))

  /**
   * param for metric name in evaluation (supports `"areaUnderROC"` (default), `"areaUnderPR"`)
   * @group param
   */

  val metricName: Param[String] = {
    val allowedParams = ParamValidators.inArray(Array("areaUnderROC", "areaUnderPR"))
    new Param(
      this, "metricName", "metric name in evaluation (areaUnderROC|areaUnderPR)", allowedParams)
  }

  /** @group getParam */

  def getMetricName: String = $(metricName)

  /** @group setParam */

  def setMetricName(value: String): this.type = set(metricName, value)

  /** @group setParam */

  def setRawPredictionCol(value: String): this.type = set(rawPredictionCol, value)

  /** @group setParam */

  def setLabelCol(value: String): this.type = set(labelCol, value)

  setDefault(metricName -> "areaUnderROC")


  override def evaluate(dataset: Dataset[_]): Double = {
    val schema = dataset.schema
    SchemaUtils.checkColumnTypes(schema, $(rawPredictionCol), Seq(DoubleType, new VectorUDT))
    SchemaUtils.checkNumericType(schema, $(labelCol))

    val summary = new BinaryClassificationSummaryImpl(dataset.toDF(), $(rawPredictionCol), $(labelCol))
    val metrics = summary.binaryMetrics

    val metric = $(metricName) match {
      case "areaUnderROC" => summary.areaUnderROC
      case "areaUnderPR" => summary.areaUnderPR
    }

    metric
  }


  override def isLargerBetter: Boolean = $(metricName) match {
    case "areaUnderROC" => true
    case "areaUnderPR" => true
  }


  override def copy(extra: ParamMap): BinaryClassificationEvaluator = defaultCopy(extra)
}


object BinaryClassificationEvaluator extends DefaultParamsReadable[BinaryClassificationEvaluator] {


  override def load(path: String): BinaryClassificationEvaluator = super.load(path)
}
