package com.tencent.angel.sona.ml.evaluation.evaluating

import com.tencent.angel.sona.ml.evaluation.BinaryClassMetrics.BinaryPredictedResult
import com.tencent.angel.sona.ml.evaluation.{BinaryClassMetrics, BinaryClassificationSummary}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}


class BinaryClassificationSummaryImpl(df: DataFrame,
                                      probabilityCol: String,
                                      labelCol: String)
  extends BinaryClassificationSummary with Serializable with Logging {

  private lazy val data: RDD[BinaryPredictedResult] = df.select(probabilityCol, labelCol).rdd.map {
    case Row(probability: Double, label: Double) =>
      BinaryPredictedResult(probability, label.toInt)
  }

  lazy val binaryMetrics: BinaryClassMetrics = data.aggregate(new BinaryClassMetrics)(
    seqOp = (metrics: BinaryClassMetrics, pres: BinaryPredictedResult) => metrics.add(pres),
    combOp = (metrics1: BinaryClassMetrics, metrics2: BinaryClassMetrics) => metrics1.merge(metrics2)
  )

  protected lazy val (tp: Double, fp: Double, fn: Double, tn: Double) = (
    binaryMetrics.getTP, binaryMetrics.getFP, binaryMetrics.getFN, binaryMetrics.getTN)
}


