package org.apache.spark.angelml.metaextract


import breeze.linalg.SparseVector
import org.apache.spark.TaskContext
import org.apache.spark.angelml.linalg.{DenseVector, IntSparseVector, Vector, Vectors}
import org.apache.spark.angelml.util.DataUtils.Example

import scala.collection.mutable


class AngelFeatureMeta extends Serializable {

  private var n = 0
  private var maxIdx: Long = Long.MinValue
  private var minIdx: Long = Long.MaxValue
  private var sparse: Option[Boolean] = None
  private var partitionCount: mutable.HashMap[Int, Int] = _
  private var currMean: Array[Double] = _
  private var currM2n: Array[Double] = _
  private var currM2: Array[Double] = _
  private var currL1: Array[Double] = _
  private var totalCnt: Long = 0
  private var totalWeightSum: Double = 0.0
  private var weightSquareSum: Double = 0.0
  private var weightSum: Array[Double] = _
  private var nnz: Array[Long] = _
  private var currMax: Array[Double] = _
  private var currMin: Array[Double] = _

  def isSparse: Boolean = sparse.getOrElse(true)

  // scalar statistics
  def maxIndex: Long = maxIdx

  def minIndex: Long = minIdx

  def validateIndexCount: Long = {
    var count: Long = 0
    currMean.foreach{ value => if (value != 0) count +=1 }

    count
  }

  def mean: Vector = {
    require(totalWeightSum > 0, s"Nothing has been added to this summarizer.")

    val realMean = Array.ofDim[Double](n)
    var i = 0
    while (i < n) {
      realMean(i) = currMean(i) * (weightSum(i) / totalWeightSum)
      i += 1
    }
    Vectors.dense(realMean)
  }

  def variance: Vector = {
    require(totalWeightSum > 0, s"Nothing has been added to this summarizer.")

    val realVariance = Array.ofDim[Double](n)

    val denominator = totalWeightSum - (weightSquareSum / totalWeightSum)

    // Sample variance is computed, if the denominator is less than 0, the variance is just 0.
    if (denominator > 0.0) {
      val deltaMean = currMean
      var i = 0
      val len = currM2n.length
      while (i < len) {
        // We prevent variance from negative value caused by numerical error.
        realVariance(i) = math.max((currM2n(i) + deltaMean(i) * deltaMean(i) * weightSum(i) *
          (totalWeightSum - weightSum(i)) / totalWeightSum) / denominator, 0.0)
        i += 1
      }
    }
    Vectors.dense(realVariance)
  }

  def count: Long = totalCnt

  // vector statistics
  def numNonzeros: Vector = {
    require(totalCnt > 0, s"Nothing has been added to this summarizer.")

    Vectors.dense(nnz.map(_.toDouble))
  }

  def max: Vector = {
    require(totalWeightSum > 0, s"Nothing has been added to this summarizer.")

    var i = 0
    while (i < n) {
      if ((nnz(i) < totalCnt) && (currMax(i) < 0.0)) currMax(i) = 0.0
      i += 1
    }
    Vectors.dense(currMax)
  }

  def min: Vector = {
    require(totalWeightSum > 0, s"Nothing has been added to this summarizer.")

    var i = 0
    while (i < n) {
      if ((nnz(i) < totalCnt) && (currMin(i) > 0.0)) currMin(i) = 0.0
      i += 1
    }
    Vectors.dense(currMin)
  }

  def normL2: Vector = {
    require(totalWeightSum > 0, s"Nothing has been added to this summarizer.")

    val realMagnitude = Array.ofDim[Double](n)

    var i = 0
    val len = currM2.length
    while (i < len) {
      realMagnitude(i) = math.sqrt(currM2(i))
      i += 1
    }
    Vectors.dense(realMagnitude)
  }

  def normL1: Vector = {
    require(totalWeightSum > 0, s"Nothing has been added to this summarizer.")

    Vectors.dense(currL1)
  }

  def partitionStat: Map[Int, Int] = partitionCount.toMap

  def add(instance: Example): this.type = {
    val feats = instance.features
    val label = instance.label
    val weight = instance.weight

    if (sparse.isEmpty) {
      val sp = feats match {
        case DenseVector(_) => false
        case IntSparseVector(_) => true
      }

      sparse = Some(sp)
    }

    require(weight >= 0.0, s"sample weight, $weight has to be >= 0.0")
    if (weight == 0.0) return this

    if (n == 0) {
      n =  feats.size.toInt
      require(n > 0, s"Vector should have dimension larger than zero.")

      partitionCount = new mutable.HashMap[Int, Int]()
      currMean = Array.ofDim[Double](n)
      currM2n = Array.ofDim[Double](n)
      currM2 = Array.ofDim[Double](n)
      currL1 = Array.ofDim[Double](n)
      weightSum = Array.ofDim[Double](n)
      nnz = Array.ofDim[Long](n)
      currMax = Array.fill[Double](n)(Double.MinValue)
      currMin = Array.fill[Double](n)(Double.MaxValue)
    }

    require(n == feats.size, s"Dimensions mismatch when adding new sample." +
      s" Expecting $n but got ${feats.size}.")

    val localCurrMean = currMean
    val localCurrM2n = currM2n
    val localCurrM2 = currM2
    val localCurrL1 = currL1
    val localWeightSum = weightSum
    val localNumNonzeros = nnz
    val localCurrMax = currMax
    val localCurrMin = currMin
    feats.foreachActive { (index, value) =>
      if (index.toInt > maxIdx) maxIdx = index.toInt
      if (index.toInt < minIdx) minIdx = index.toInt

      if (value != 0.0) {
        if (localCurrMax(index.toInt) < value) {
          localCurrMax(index.toInt) = value
        }
        if (localCurrMin(index.toInt) > value) {
          localCurrMin(index.toInt) = value
        }

        val prevMean = localCurrMean(index.toInt)
        val diff = value - prevMean
        localCurrMean(index.toInt) = prevMean + weight * diff / (localWeightSum(index.toInt) + weight)
        localCurrM2n(index.toInt) += weight * (value - localCurrMean(index.toInt)) * diff
        localCurrM2(index.toInt) += weight * value * value
        localCurrL1(index.toInt) += weight * math.abs(value)

        localWeightSum(index.toInt) += weight
        localNumNonzeros(index.toInt) += 1
      }
    }

    totalWeightSum += weight
    weightSquareSum += weight * weight
    totalCnt += 1

    partitionCount.put(TaskContext.getPartitionId(), totalCnt.toInt)

    this
  }

  def merge(other: AngelFeatureMeta): this.type = {
    if (maxIdx < other.maxIdx) {
      this.maxIdx = other.maxIdx
    }

    if (minIdx > other.minIdx) {
      this.minIdx = other.minIdx
    }

    if (partitionCount == null) {
      partitionCount = new mutable.HashMap[Int, Int]()
    }

    if (other.partitionCount != null) {
      partitionCount ++= other.partitionCount
    }


    if (this.totalWeightSum != 0.0 && other.totalWeightSum != 0.0) {
      require(n == other.n, s"Dimensions mismatch when merging with another summarizer. " +
        s"Expecting $n but got ${other.n}.")
      totalCnt += other.totalCnt
      totalWeightSum += other.totalWeightSum
      weightSquareSum += other.weightSquareSum

      var i = 0
      while (i < n) {
        val thisNnz = weightSum(i)
        val otherNnz = other.weightSum(i)
        val totalNnz = thisNnz + otherNnz
        val totalCnnz = nnz(i) + other.nnz(i)
        if (totalNnz != 0.0) {
          val deltaMean = other.currMean(i) - currMean(i)
          // merge mean together
          currMean(i) += deltaMean * otherNnz / totalNnz
          // merge m2n together
          currM2n(i) += other.currM2n(i) + deltaMean * deltaMean * thisNnz * otherNnz / totalNnz
          // merge m2 together
          currM2(i) += other.currM2(i)
          // merge l1 together
          currL1(i) += other.currL1(i)
          // merge max and min
          currMax(i) = math.max(currMax(i), other.currMax(i))
          currMin(i) = math.min(currMin(i), other.currMin(i))
        }
        weightSum(i) = totalNnz
        nnz(i) = totalCnnz
        i += 1
      }
    } else if (totalWeightSum == 0.0 && other.totalWeightSum != 0.0) {
      this.n = other.n
      this.currMean = other.currMean.clone()
      this.currM2n = other.currM2n.clone()
      this.currM2 = other.currM2.clone()
      this.currL1 = other.currL1.clone()
      this.totalCnt = other.totalCnt
      this.totalWeightSum = other.totalWeightSum
      this.weightSquareSum = other.weightSquareSum
      this.weightSum = other.weightSum.clone()
      this.nnz = other.nnz.clone()
      this.currMax = other.currMax.clone()
      this.currMin = other.currMin.clone()
    }
    this
  }
}
