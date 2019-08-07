package org.apache.spark.angel.ml.evaluation

import com.tencent.angel.mlcore.PredictResult
import org.apache.spark.angel.ml.evaluation.training.ClassificationTrainingStat

import scala.collection.mutable
import scala.language.implicitConversions

abstract class TrainingStat extends Serializable {
  protected var avgLoss: Double = 0.0
  protected var numSamples: Long = 0L
  protected var createTime: Long = 0L
  protected var initTime: Long = 0L
  protected var loadTime: Long = 0L
  protected var pullTime: Long = 0L
  protected var pushTime: Long = 0L
  protected var updateTime: Long = 0L
  protected var forwardTime: Long = 0L
  protected var backwardTime: Long = 0L

  @transient protected var histLoss: mutable.ListBuffer[Double] = _

  def setHistLoss(hist: mutable.ListBuffer[Double]): this.type = {
    if (hist == null) {
      histLoss = null
    } else if (hist.isEmpty) {
      histLoss.clear()
    } else {
      if (histLoss != null) {
        histLoss.clear()
      } else {
        histLoss = new mutable.ListBuffer[Double]()
      }

      hist.foreach(item => histLoss.append(item))
    }

    this
  }

  def addHistLoss(): this.type = {
    if (histLoss == null) {
      histLoss = new mutable.ListBuffer[Double]()
    }

    histLoss.append(getAvgLoss)
    this
  }

  def getHistLoss: Array[Double] = {
    if (histLoss != null) {
      histLoss.toArray
    } else {
      null.asInstanceOf[Array[Double]]
    }
  }

  def getAvgLoss: Double = avgLoss

  def getNumSamples: Long = numSamples

  def getCreateTime: Long = createTime

  def getInitTime: Long = initTime

  def getLoadTime: Long = loadTime

  def getPullTime: Long = pullTime

  def getPushTime: Long = pushTime

  def getUpdateTime: Long = updateTime

  def getForwardTime: Long = forwardTime

  def getBackwardTime: Long = backwardTime

  def setAvgLoss(loss: Double): this.type = {
    avgLoss = loss
    this
  }

  def setNumSamples(num: Long): this.type = {
    numSamples = num
    this
  }

  def setCreateTime(time: Long): this.type = {
    createTime = time
    this
  }

  def setInitTime(time: Long): this.type = {
    initTime = time
    this
  }

  def setLoadTime(time: Long): this.type = {
    loadTime = time
    this
  }

  def setPullTime(time: Long): this.type = {
    pullTime = time
    this
  }

  def setPushTime(time: Long): this.type = {
    pushTime = time
    this
  }

  def setUpdateTime(time: Long): this.type = {
    updateTime = time
    this
  }

  def setForwardTime(time: Long): this.type = {
    forwardTime = time
    this
  }

  def setBackwardTime(time: Long): this.type = {
    backwardTime = time
    this
  }

  def merge(other: TrainingStat): this.type = mergeSum(other)

  protected def mergePart(other: TrainingStat): Unit = {
    avgLoss = (avgLoss * numSamples + other.avgLoss * other.numSamples) / (numSamples + other.numSamples)
    numSamples += other.numSamples
  }

  def mergeSum(other: TrainingStat): this.type = {
    mergePart(other)

    createTime += other.createTime
    initTime += other.initTime
    loadTime += other.loadTime
    pullTime += other.pullTime
    pushTime += other.pushTime
    updateTime += other.updateTime
    forwardTime += other.forwardTime
    backwardTime += other.backwardTime

    this
  }

  def mergeMax(other: TrainingStat): this.type = {
    mergePart(other)

    createTime = Math.max(createTime, other.createTime)
    initTime = Math.max(initTime, other.initTime)
    loadTime = Math.max(loadTime, other.loadTime)
    pullTime = Math.max(pullTime, other.pullTime)
    pushTime = Math.max(pushTime, other.pushTime)
    updateTime = Math.max(updateTime, other.updateTime)
    forwardTime = Math.max(forwardTime, other.forwardTime)
    backwardTime = Math.max(backwardTime, other.backwardTime)

    this
  }

  def printString(): String

  protected def printString(insert: String): String = {
    f"avgLoss=$avgLoss%.6f, $insert, numSamples=$numSamples%-8d " +
      f"pull=$pullTime%-6d push=$pushTime%-6d update=$updateTime%-6d " +
      f"forward=$forwardTime%-6d backward=$backwardTime%-6d"
  }

  def add(pres: PredictResult): this.type

  def clearStat(): this.type
}

object TrainingStat extends Serializable {

  def mergeInBatch[T<: TrainingStat](rs1: T, rs2: T): T = { rs1.mergeMax(rs2) }

  def merge[T<: TrainingStat](rs1: T, rs2: T): T = { rs1.mergeSum(rs2) }

  implicit def toBinaryClassificationSummary(stat: TrainingStat): BinaryClassificationSummary = {
    stat match {
      case s: ClassificationTrainingStat =>
        s.getSummary match {
          case summary: BinaryClassificationSummary => summary
        }
    }
  }

  implicit def toClassificationSummary(stat: TrainingStat): ClassificationSummary = {
    stat match {
      case s: ClassificationTrainingStat =>
        s.getSummary match {
          case summary: ClassificationSummary => summary
        }
    }
  }
}
