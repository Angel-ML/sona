package com.tencent.angel.sona.common.measure.training

import com.tencent.angel.ml.core.PredictResult
import com.tencent.angel.sona.common.measure.TrainingStat

class ClusteringTrainingStat extends TrainingStat {
  override def printString(): String = ???

  override def add(pres: PredictResult): ClusteringTrainingStat.this.type = ???

  override def clearStat(): ClusteringTrainingStat.this.type = ???
}
