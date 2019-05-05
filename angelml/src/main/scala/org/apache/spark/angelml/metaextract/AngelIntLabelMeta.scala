package org.apache.spark.angelml.metaextract


import org.apache.spark.angelml.util.DataUtils.Example

import scala.collection.mutable

class AngelIntLabelMeta extends Serializable {
  private val distinctMap = new mutable.HashMap[Int, (Long, Double)]
  private var totalInvalidCnt: Long = 0L

  def minLabel: Int = distinctMap.keys.min

  def countInvalid: Long = totalInvalidCnt

  def numClasses: Int = if (distinctMap.isEmpty) 0 else distinctMap.size

  def histogram: Array[Double] = {
    val result = Array.ofDim[Double](numClasses)
    var i = 0
    val len = result.length
    while (i < len) {
      result(i) = distinctMap.getOrElse(i, (0L, 0.0))._2
      i += 1
    }
    result
  }

  def add(instance: Example): this.type = {
    val label = instance.label

    require(instance.weight >= 0.0, s"instance weight, ${instance.weight} has to be >= 0.0")
    val weight: Double = if (instance.weight == 0.0) 1.0 else instance.weight

    if (label - label.toInt != 0.0 || label < -1) {
      totalInvalidCnt += 1
      this
    } else {
      val (counts: Long, weightSum: Double) = distinctMap.getOrElse(label.toInt, (0L, 0.0))
      distinctMap.put(label.toInt, (counts + 1L, weightSum + weight))
      this
    }
  }

  def merge(other: AngelIntLabelMeta): AngelIntLabelMeta = {
    val (largeMap, smallMap) = if (this.distinctMap.size > other.distinctMap.size) {
      (this, other)
    } else {
      (other, this)
    }
    smallMap.distinctMap.foreach {
      case (key, value) =>
        val (counts: Long, weightSum: Double) = largeMap.distinctMap.getOrElse(key, (0L, 0.0))
        largeMap.distinctMap.put(key, (counts + value._1, weightSum + value._2))
    }
    largeMap.totalInvalidCnt += smallMap.totalInvalidCnt
    largeMap
  }

}
