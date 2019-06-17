package org.apache.spark.angelml.evaluation.training

import com.tencent.angel.ml.core.PredictResult
import org.apache.spark.angelml.evaluation.TrainingStat

class ClusteringTrainingStat extends TrainingStat {
  override def printString(): String = ???

  override def add(pres: PredictResult): ClusteringTrainingStat.this.type = ???

  override def clearStat(): ClusteringTrainingStat.this.type = ???
}
