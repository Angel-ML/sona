package org.apache.spark.angelml.metaextract

import org.apache.spark.angelml.util.DataUtils.Example

class AngelFloatLabelMeta extends Serializable {
  private var currSum: Double = 0.0
  private var currM2: Double = 0.0
  private var currMax: Double = Double.MinValue
  private var currMin: Double = Double.MaxValue
  private var count: Long = 0
  private var totalWeight: Double = 0.0

  def maxValue: Double = currMax

  def minValue: Double = currMin

  def mean: Double = currSum / totalWeight

  def variance: Double = {
    val currMean = currSum / totalWeight
    val currMean2 = currM2 / totalWeight
    currMean2 - currMean * currMean
  }

  def std: Double = Math.sqrt(variance)

  def add(instance: Example): this.type = {
    val label = instance.label

    require(instance.weight >= 0.0, s"instance weight, ${instance.weight} has to be >= 0.0")
    val weight: Double = if (instance.weight == 0.0) 1.0 else instance.weight
    currSum += weight * label
    currM2 += weight * label * label

    if (label > currMax) {
      currMax = label
    }

    if (label < currMin) {
      currMax = label
    }

    count += 1
    totalWeight += weight

    this
  }

  def merge(other: AngelFloatLabelMeta): AngelFloatLabelMeta = {
    currSum += other.currSum
    currM2 += other.currM2

    if (other.currMin < currMin) {
      currMin = other.currMin
    }

    if (other.currMax > currMax) {
      currMax = other.currMax
    }

    count += other.count
    totalWeight += other.totalWeight
    this
  }
}
