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
package org.apache.spark.angel.ml.evaluation

abstract class RegressionSummary {
  val regMetrics : RegressionMetrics

  def meanLabel: Double = {
    regMetrics.currLabelSum / regMetrics.count
  }

  def varLabel: Double = {
    regMetrics.currLabelSum2 / regMetrics.count - Math.pow(meanLabel, 2)
  }

  def stdLabel: Double = Math.sqrt(varLabel)

  def meanPred: Double = {
    regMetrics.currPredSum / regMetrics.count
  }

  def varPred: Double = {
    regMetrics.currPredSum2 / regMetrics.count - Math.pow(meanPred, 2)
  }

  def stdPred: Double = Math.sqrt(varPred)

  def explainedVariance: Double = {
    // explainedVariance = SSReg / n
    val meanLabel_t = meanLabel
    regMetrics.currPredSum2 / regMetrics.count + meanLabel_t * (meanLabel_t - 2 * meanPred)
  }

  def mse: Double = {
    (regMetrics.currLabelSum2 + regMetrics.currPredSum2 - 2 * regMetrics.currPredLabelSum) / regMetrics.count
  }

  def rmse: Double = Math.sqrt(mse)

  lazy val absDiff: Double = {
    regMetrics.currPredLabelDiffAbs / regMetrics.count
  }

  def r2: Double = {
    val (slope_t, intercept_t, meanLabel_t) = (slope, intercept, meanLabel)
    val (currLabelSum, currPredSum) = (regMetrics.currLabelSum, regMetrics.currPredSum)
    val (currLabelSum2, currPredSum2) = (regMetrics.currLabelSum2, regMetrics.currPredSum2)
    val (currPredLabelSum, count) = (regMetrics.currPredLabelSum, regMetrics.count)

    val square = currLabelSum2 + slope_t * slope_t * currPredSum2 + intercept_t * intercept_t * count
    val cross = slope_t * intercept_t * currPredSum - intercept_t * currLabelSum - slope_t * currPredLabelSum
    val SSE = square + 2 * cross
    val SSTot = currLabelSum2 - count * meanLabel_t * meanLabel_t
    1.0 - SSE / SSTot
  }

  def slope: Double = {
    val (meanLabel_t, meanPred_t) = (meanLabel, meanPred)
    val numerator = regMetrics.currPredLabelSum / regMetrics.count - meanLabel_t * meanPred_t
    val denominator = regMetrics.currPredSum2 / regMetrics.count - meanPred_t * meanPred_t

    numerator / denominator
  }

  def intercept: Double = {
    meanLabel - meanPred * slope
  }
  
}
