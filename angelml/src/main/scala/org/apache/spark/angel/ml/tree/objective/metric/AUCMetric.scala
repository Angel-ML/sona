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
