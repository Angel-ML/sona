package com.tencent.angel.sona.ml.evaluation

import scala.collection.mutable

class MultiClassMetrics extends Serializable {

  import MultiClassMetrics.MultiPredictedResult

  lazy val classCount: mutable.HashMap[Int, Long] = new mutable.HashMap[Int, Long]()

  lazy val tpByClass: mutable.HashMap[Int, Long] = new mutable.HashMap[Int, Long]()

  lazy val fpByClass: mutable.HashMap[Int, Long] = new mutable.HashMap[Int, Long]()

  lazy val confusions: mutable.HashMap[(Int, Int), Long] = new mutable.HashMap[(Int, Int), Long]()

  def add(pres: MultiPredictedResult): this.type = {
    val label = pres.label
    val prediction = pres.prediction

    val classCountAddOne = 1 + classCount.getOrElse(label, 0.asInstanceOf[Long])
    classCount.put(label, classCountAddOne)

    if (label == prediction) {
      val tpByClassAddOne = 1 + tpByClass.getOrElse(label, 0.asInstanceOf[Long])
      tpByClass.put(label, tpByClassAddOne)
    } else {
      val fpByClassAddOne = 1 + fpByClass.getOrElse(prediction, 0.asInstanceOf[Long])
      fpByClass.put(prediction, fpByClassAddOne)
    }

    val fusionKey = (label, prediction)
    val confusionsAddOne = 1 + confusions.getOrElse(fusionKey, 0.asInstanceOf[Long])
    confusions.put(fusionKey, confusionsAddOne)

    this
  }

  def merge(other: MultiClassMetrics): this.type = {
    val labelKeys = classCount.keySet ++ other.classCount.keySet
    labelKeys.foreach {
      case key if classCount.contains(key) && other.classCount.contains(key) =>
        classCount.put(key, classCount(key) + other.classCount(key))
      case key if !classCount.contains(key) && other.classCount.contains(key) =>
        classCount.put(key, other.classCount(key))
      case _ =>
    }

    val tpKeys = tpByClass.keySet ++ other.tpByClass.keySet
    tpKeys.foreach {
      case key if tpByClass.contains(key) && other.tpByClass.contains(key) =>
        tpByClass.put(key, tpByClass(key) + other.tpByClass(key))
      case key if !tpByClass.contains(key) && other.tpByClass.contains(key) =>
        tpByClass.put(key, other.tpByClass(key))
      case _ =>
    }

    val fpKeys = fpByClass.keySet ++ other.fpByClass.keySet
    fpKeys.foreach {
      case key if fpByClass.contains(key) && other.fpByClass.contains(key) =>
        fpByClass.put(key, fpByClass(key) + other.fpByClass(key))
      case key if !fpByClass.contains(key) && other.fpByClass.contains(key) =>
        fpByClass.put(key, other.fpByClass(key))
      case _ =>
    }

    val confusionKeys = confusions.keySet ++ other.confusions.keySet
    confusionKeys.foreach {
      case key if confusions.contains(key) && other.confusions.contains(key) =>
        confusions.put(key, confusions(key) + other.confusions(key))
      case key if !confusions.contains(key) && other.confusions.contains(key) =>
        confusions.put(key, other.confusions(key))
      case _ =>
    }

    this
  }

  def clear(): this.type = {
    classCount.clear()
    tpByClass.clear()
    fpByClass.clear()
    confusions.clear()
    this
  }

}

object MultiClassMetrics {

  case class MultiPredictedResult(prediction: Int, label: Int) extends Serializable

}

