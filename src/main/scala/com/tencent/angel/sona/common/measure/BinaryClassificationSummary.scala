package com.tencent.angel.sona.common.measure

import org.apache.spark.internal.Logging

abstract class BinaryClassificationSummary extends ClassificationSummary with Serializable with Logging {

  val binaryMetrics: BinaryClassMetrics

  protected val tp: Double
  protected val fp: Double
  protected val fn: Double
  protected val tn: Double

  override def truePositiveRate(label: Double = 0.0): Double = {
    tp / (tp + fn)
  }

  override def falsePositiveRate(label: Double = 0.0): Double = {
    fp / (fp + tn)
  }

  override def precision(label: Double = 0.0): Double = {
    tp / (tp + fp)
  }

  override def recall(label: Double = 0.0): Double = truePositiveRate()

  override def fMeasure(beta: Double)(label: Double = 0.0): Double = {
    val (p: Double, r: Double) = (precision(), recall())
    val beta2 = Math.pow(beta, 2)

    (beta2 + 1) * p * r / (beta2 * p + r)
  }

  override def accuracy: Double = {
    (tp + tn) / (tp + fp + fn + tn)
  }

  @transient lazy val roc: Array[(Double, Double)] = {
    // +, -
    val binStat = binaryMetrics.getBinStat

    var trueCount: Double = 0
    var falseCount: Double = 0
    binStat.foreach {
      case Array(pos, neg) =>
        trueCount += pos
        falseCount += neg
    }

    var (currTP: Double, currFP: Double, currFN: Double, currTN: Double) =
      (trueCount, falseCount, 0.0, 0.0)


    val rocData: Array[(Double, Double)] = new Array[(Double, Double)](binStat.length + 2)
    rocData(0) = (0.0, 0.0)
    rocData(binStat.length + 1) = (1.0, 1.0)
    binStat.zipWithIndex.foreach {
      case (Array(pos, neg), idx: Int) =>
        currTP -= pos
        currFP -= neg
        currFN += pos
        currTN += neg

        if (currTP + currFN == 0 || currTN + currFP == 0) {
          rocData(idx + 1) = (0.0, 0.0)
        } else {
          val tpr = currTP / (currTP + currFN)
          val fpr = currFP / (currTN + currFP)

          rocData(idx + 1) = (fpr, tpr)
        }
    }

    rocData.sortWith { (p1: (Double, Double), p2: (Double, Double)) => p1._1 <= p2._1 }
  }

  @transient lazy val areaUnderROC: Double = {
    var prAUC = 0.0
    roc.sliding(2).foreach {
      case Array((x1: Double, y1: Double), (x2: Double, y2: Double)) =>
        prAUC += (x2 - x1) * (y1 + y2) / 2.0
    }

    prAUC
  }

  @transient lazy val pr: Array[(Double, Double)] = {
    // +, -
    val binStat = binaryMetrics.getBinStat

    var trueCount: Double = 0
    var falseCount: Double = 0
    binStat.foreach {
      case Array(pos, neg) =>
        trueCount += pos
        falseCount += neg
    }

    var (currTP: Double, currFP: Double, currFN: Double, currTN: Double) =
      (trueCount, falseCount, 0.0, 0.0)


    val rocData: Array[(Double, Double)] = new Array[(Double, Double)](binStat.length + 2)
    rocData(0) = (0.0, 1.0)
    rocData(binStat.length + 1) = (1.0, 0.0)
    binStat.zipWithIndex.foreach {
      case (Array(pos, neg), idx: Int) =>
        currTP -= pos
        currFP -= neg
        currFN += pos
        currTN += neg

        if (currTP + currFN == 0 || currTP + currFP == 0) {
          rocData(idx + 1) = (1.0, 0.0)
        } else {
          val currRecall = currTP / (currTP + currFN)
          val currPrecision = currTP / (currTP + currFP)

          rocData(idx + 1) = (currRecall, currPrecision)
        }
    }

    rocData.sortWith { (p1: (Double, Double), p2: (Double, Double)) => p1._1 <= p2._1 }
  }

  @transient lazy val areaUnderPR: Double = {
    var rocAUC = 0.0
    pr.sliding(2).foreach {
      case Array((x1: Double, y1: Double), (x2: Double, y2: Double)) =>
        rocAUC += (x2 - x1) * (y1 + y2) / 2.0
    }

    rocAUC
  }
}
