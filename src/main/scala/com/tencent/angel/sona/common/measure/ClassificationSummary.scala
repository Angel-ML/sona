package com.tencent.angel.sona.common.measure

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