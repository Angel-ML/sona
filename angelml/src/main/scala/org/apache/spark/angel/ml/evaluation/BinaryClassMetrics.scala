package org.apache.spark.angel.ml.evaluation

class BinaryClassMetrics extends Serializable {

  import BinaryClassMetrics.BinaryPredictedResult

  private val numBins: Int = 100
  private val binStat = (0 until numBins).toArray.map { _ => new Array[Double](2) }
  private var tp: Double = 0L
  private var fp: Double = 0L
  private var fn: Double = 0L
  private var tn: Double = 0L

  def getBinStat: Array[Array[Double]] = binStat

  def getTP: Double = tp

  def getFP: Double = fp

  def getFN: Double = fn

  def getTN: Double = tn

  def add(pres: BinaryPredictedResult): this.type = {
    val probability = pres.probability
    if (probability >= 0 && probability <= 1) {
      val label = pres.label
      val binIdx = Math.round(probability * numBins).toInt % numBins
      val binCount: Array[Double] = binStat(binIdx)
      if (pres.label >= 0.5) {
        binCount(0) += 1
      } else {
        binCount(1) += 1
      }

      if (probability >= 0.5 && label >= 0.5) {
        tp += 1
      } else if (probability >= 0.5 && label < 0.5) {
        fp += 1
      } else if (probability < 0.5 && label >= 0.5) {
        fn += 1
      } else {
        tn += 1
      }
    }

    this
  }

  def merge(other: BinaryClassMetrics): this.type = {

    binStat.indices.foreach { idx =>
      val thisBinCount: Array[Double] = binStat(idx)
      val thatBinCount: Array[Double] = other.binStat(idx)

      thisBinCount(0) += thatBinCount(0)
      thisBinCount(1) += thatBinCount(1)
    }

    tp += other.tp
    fp += other.fp
    fn += other.fn
    tn += other.tn

    this
  }

  def clear(): this.type = {
    tp = 0
    fp = 0
    fn = 0
    tn = 0

    binStat.foreach { row =>
      row(0) = 0
      row(1) = 0
    }

    this
  }

}

object BinaryClassMetrics {

  case class BinaryPredictedResult(probability: Double, label: Int) extends Serializable

}
