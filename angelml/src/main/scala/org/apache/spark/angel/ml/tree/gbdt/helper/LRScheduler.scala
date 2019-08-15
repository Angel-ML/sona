package org.apache.spark.angel.ml.tree.gbdt.helper

import org.apache.spark.angel.ml.tree.gbdt.helper.LRScheduler._
import org.apache.spark.angel.ml.tree.objective.ObjectiveFactory
import org.apache.spark.angel.ml.tree.objective.metric.EvalMetric

object LRScheduler {

  private def isBetter(kind: EvalMetric.Kind, former: Double, latter: Double,
                       threshold: Double): Boolean = {
    kind match {
      case EvalMetric.Kind.AUC | EvalMetric.Kind.PRECISION =>
        latter > former && Math.abs(latter - former) / former > threshold
      case EvalMetric.Kind.CROSS_ENTROPY | EvalMetric.Kind.ERROR
           | EvalMetric.Kind.LOG_LOSS | EvalMetric.Kind.RMSE =>
        latter < former && Math.abs(latter - former) / former > threshold
      case _ => throw new RuntimeException(s"Unrecognizable eval metric: $kind")
    }
  }

  /** -------------------- Default hyper-parameters -------------------- */
  val DEFAULT_PATIENT: Int = 5
  val DEFAULT_THRESHOLD: Double = 1e-4  // 0.01%
  val DEFAULT_DECAY_FACTOR: Float = 0.1f
  val DEFAULT_EARLY_STOP: Int = 3
}

class LRScheduler(maxSteps: Int, evalMetricsStr: Seq[String], dataset: String = "valid",
                  patient: Int = DEFAULT_PATIENT, threshold: Double = DEFAULT_THRESHOLD,
                  decayFactor: Float = DEFAULT_DECAY_FACTOR,
                  earlyStop: Int = DEFAULT_EARLY_STOP) extends Serializable {
  require(dataset == "train" || dataset == "valid", s"dataset should be 'train' or 'valid', but got $dataset")

  private var currentStep: Int = 0
  private val evalMetrics: Seq[EvalMetric.Kind] = evalMetricsStr.map(ObjectiveFactory.getEvalMetricKind)
  private val chiefMetric: EvalMetric.Kind = evalMetrics.head
  private val history: Map[String, Map[EvalMetric.Kind, Array[Double]]] = Map(
    "train" -> evalMetrics.map(kind => kind -> Array.ofDim[Double](maxSteps)).toMap,
    "valid" -> evalMetrics.map(kind => kind -> Array.ofDim[Double](maxSteps)).toMap)
  private var bestStep: Int = -1
  private var bestMetric: Double = Double.NaN

  def step(metrics: Seq[(EvalMetric.Kind, Double, Double)], currentLR: Float): Float = {
    metrics.foreach {
      case (kind, train, valid) =>
        history("train")(kind)(currentStep) = train
        history("valid")(kind)(currentStep) = valid
    }
    currentStep += 1

    val currentMetric = history(dataset)(chiefMetric)(currentStep - 1)
    if (bestStep == -1 || isBetter(chiefMetric, bestMetric, currentMetric, threshold)) {
      bestStep = currentStep
      bestMetric = currentMetric
    }

    if (shouldEarlyStop()) {
      println(s"$chiefMetric does not improve for ${patient * earlyStop} " +
        s"rounds, early stopping")
      0f
    } else if (shouldDecay()) {
      val decayedLR = currentLR * decayFactor
      println(s"$chiefMetric does not improve for $patient " +
        s"rounds, decay learning rate from $currentLR to $decayedLR")
      decayedLR
    } else {
      currentLR
    }
  }

  def shouldDecay(): Boolean = patient > 0 && currentStep - bestStep == patient

  def shouldEarlyStop(): Boolean = earlyStop > 0 && patient > 0 &&
    currentStep - bestStep >= patient * earlyStop

  def getHistory: Map[String, Map[EvalMetric.Kind, Array[Double]]] = history

  def getBest: (EvalMetric.Kind, Int, Double) = (chiefMetric, bestStep, bestMetric)
}
