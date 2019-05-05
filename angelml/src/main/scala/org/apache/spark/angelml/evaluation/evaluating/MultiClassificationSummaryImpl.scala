package org.apache.spark.angelml.evaluation.evaluating

import org.apache.spark.angelml.evaluation.MultiClassMetrics.MultiPredictedResult
import org.apache.spark.angelml.evaluation.{MultiClassMetrics, MultiClassificationSummary}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}

class MultiClassificationSummaryImpl(df: DataFrame, predictionCol: String, labelCol: String)
  extends MultiClassificationSummary with Serializable with Logging {

  private lazy val data: RDD[MultiPredictedResult] = df.select(predictionCol, labelCol).rdd.map {
    case Row(predictediction: Double, label: Double) =>
      MultiPredictedResult(predictediction.toInt, label.toInt)
  }

  lazy val multiMetrics: MultiClassMetrics = data.aggregate(new MultiClassMetrics)(
    seqOp = (metrics: MultiClassMetrics, pres: MultiPredictedResult) => metrics.add(pres),
    combOp = (metrics1: MultiClassMetrics, metrics2: MultiClassMetrics) => metrics1.merge(metrics2)
  )
}
