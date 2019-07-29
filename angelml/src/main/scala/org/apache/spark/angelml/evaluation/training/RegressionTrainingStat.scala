package org.apache.spark.angelml.evaluation.training

import com.tencent.angel.mlcore.PredictResult
import org.apache.spark.angelml.evaluation.RegressionMetrics.RegressionPredictedResult
import org.apache.spark.angelml.evaluation.{RegressionMetrics, RegressionSummary, TrainingStat}

class RegressionTrainingStat extends TrainingStat with Serializable {
  lazy val stat = new RegressionMetrics

  def getSummary: RegressionSummary = {
    new RegressionSummary{
      override val regMetrics: RegressionMetrics = stat
    }
  }

  override def printString(): String = {
    val summary = getSummary
    val insert = f"MSE=${summary.mse}%.3f, RMSE=${summary.rmse}%.3f, R2=${summary.r2}%.3f, " +
      f"explainedVariance=${summary.explainedVariance}%.3f"

    printString(insert)
  }

  override def add(pres: PredictResult): this.type = {
    stat.add(RegressionPredictedResult(pres.pred, pres.trueLabel))
    this
  }

  override protected def mergePart(other: TrainingStat): Unit = {
    val o = other.asInstanceOf[RegressionTrainingStat]

    stat.merge(o.stat)

    super.mergePart(other)
  }

  override def clearStat(): this.type = {
    stat.clear()

    this
  }
}
