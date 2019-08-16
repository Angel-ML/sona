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
package org.apache.spark.angel.ml.tree.objective.metric

import javax.inject.Singleton

object AUCMetric {
  private lazy val instance = new AUCMetric

  def getInstance(): AUCMetric = instance
}

@Singleton
class AUCMetric private extends EvalMetric {
  override def getKind: EvalMetric.Kind = EvalMetric.Kind.AUC

  override def eval(preds: Array[Float], labels: Array[Float]): Double = {
    requireBinary(preds, labels)
    val scores = labels.zip(preds).sortBy(_._2)

    val numTotal = scores.length
    val numPos = scores.count(_._1 > 0)
    val numNeg = numTotal - numPos

    // calculate the summation of ranks for positive samples
    val sumRanks = scores.zipWithIndex.filter(_._1._1.toInt == 1)
      .map(f => (1.0 + f._2) / numPos / numNeg).sum

    sumRanks.toFloat - (1.0 + numPos) / numNeg / 2.0
  }

  override def eval(preds: Array[Float], labels: Array[Float], start: Int, end: Int): Double = {
    requireBinary(preds, labels)
    eval(preds.slice(start, end), labels.slice(start, end))
  }

  private def requireBinary(preds: Array[Float], labels: Array[Float]): Unit = {
    if (preds.length != labels.length)
      throw new RuntimeException("AUC metric should be used for binary classification")
  }
}
