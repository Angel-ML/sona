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

import scala.language.implicitConversions


trait ClassificationSummary {
  def truePositiveRate(label: Double): Double

  def falsePositiveRate(label: Double): Double

  def precision(label: Double): Double

  def recall(label: Double): Double

  def fMeasure(beta: Double)(label: Double): Double

  def accuracy: Double
}


object ClassificationSummary {
  implicit def toBinary(summary: ClassificationSummary): BinaryClassificationSummary = {
    summary match {
      case s: BinaryClassificationSummary => s
    }
  }

  implicit def toMulti(summary: ClassificationSummary): MultiClassificationSummary = {
    summary match {
      case s: MultiClassificationSummary => s
    }
  }
}