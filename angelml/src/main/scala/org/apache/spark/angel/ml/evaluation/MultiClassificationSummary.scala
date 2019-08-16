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

import org.apache.spark.angel.ml.linalg.{Matrices, Matrix}

import scala.collection.mutable

abstract class MultiClassificationSummary extends ClassificationSummary {

  val multiMetrics: MultiClassMetrics

  private lazy val labelCountByClass: mutable.HashMap[Int, Long] = multiMetrics.classCount

  private lazy val labelCount: Long = labelCountByClass.values.sum

  private lazy val tpByClass: mutable.HashMap[Int, Long] = multiMetrics.tpByClass

  private lazy val fpByClass: mutable.HashMap[Int, Long] = multiMetrics.fpByClass

  private lazy val confusions: mutable.HashMap[(Int, Int), Long] = multiMetrics.confusions

  def confusionMatrix: Matrix = {
    val n = labelCountByClass.size
    val values = Array.ofDim[Double](n * n)
    confusions.foreach {
      case ((predicted: Int, label: Int), count: Long) =>
        values(predicted * n + label) = count
    }
    Matrices.dense(n, n, values)
  }

  override def truePositiveRate(label: Double): Double = recall(label)

  override def falsePositiveRate(label: Double): Double = {
    val fp = fpByClass.getOrElse(label.toInt, 0.asInstanceOf[Long])
    1.0 * fp / (labelCount - labelCountByClass(label.toInt))
  }

  override def precision(label: Double): Double = {
    val tp = tpByClass(label.toInt)
    val fp = fpByClass.getOrElse(label.toInt, 0L)
    if (tp + fp == 0) 0 else 1.0 * tp / (tp + fp)
  }

  override def recall(label: Double): Double = {
    tpByClass(label.toInt).toDouble / labelCountByClass(label.toInt)
  }

  override def fMeasure(beta: Double)(label: Double): Double = {
    val p = precision(label.toInt)
    val r = recall(label.toInt)
    val betaSqrd = beta * beta
    if (p + r == 0) 0 else (1 + betaSqrd) * p * r / (betaSqrd * p + r)
  }

  override def accuracy: Double = {
    val correct = confusions.map {
      case ((predicted: Int, label: Int), count: Long) if predicted == label =>
        count
      case _ => 0.0
    }.sum

    correct / labelCount
  }
}
