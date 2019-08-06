package org.apache.spark.angel.ml.evaluation.evaluating

import org.apache.spark.angel.ml.evaluation.RegressionMetrics.RegressionPredictedResult
import org.apache.spark.angel.ml.evaluation.{RegressionMetrics, RegressionSummary}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}

class RegressionSummaryImpl(df: DataFrame, predictionCol: String, labelCol: String) extends RegressionSummary with Serializable {
  private lazy val data: RDD[RegressionPredictedResult] = df.select(predictionCol, labelCol).rdd.map {
    case Row(probability: Double, label: Double) =>
      RegressionPredictedResult(probability, label.toInt)
  }

  override val regMetrics: RegressionMetrics = data.aggregate(new RegressionMetrics)(
    seqOp = (metrics: RegressionMetrics, pres: RegressionPredictedResult) => metrics.add(pres),
    combOp = (metrics1: RegressionMetrics, metrics2: RegressionMetrics) => metrics1.merge(metrics2)
  )
}
