package com.tencent.angel.sona.ml.evaluation.training

import com.tencent.angel.mlcore.PredictResult
import com.tencent.angel.sona.ml.evaluation.TrainingStat

class ClusteringTrainingStat extends TrainingStat {
  override def printString(): String = ???

  override def add(pres: PredictResult): ClusteringTrainingStat.this.type = ???

  override def clearStat(): ClusteringTrainingStat.this.type = ???
}
