package org.apache.spark.angel.ml.evaluation.training

import com.tencent.angel.mlcore.PredictResult
import org.apache.spark.angel.ml.evaluation.TrainingStat

class ClusteringTrainingStat extends TrainingStat {
  override def printString(): String = ???

  override def add(pres: PredictResult): ClusteringTrainingStat.this.type = ???

  override def clearStat(): ClusteringTrainingStat.this.type = ???
}
