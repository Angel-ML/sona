package org.apache.spark.angelml.common

import com.tencent.angel.ml.core.conf.{MLCoreConf, SharedConf}
import com.tencent.angel.ml.servingmath2.utils.LabeledData
import com.tencent.angel.sona.core.ExecutorContext
import com.tencent.angel.sona.util.ConfUtils
import org.apache.spark.angelml.evaluation.TrainingStat
import org.apache.spark.angelml.evaluation.training.{ClassificationTrainingStat, ClusteringTrainingStat, RegressionTrainingStat}
import org.apache.spark.broadcast.Broadcast

class Trainer(bcValue: Broadcast[ExecutorContext], epoch: Int, bcConf: Broadcast[SharedConf]) extends Serializable {
  @transient private lazy val executorContext: ExecutorContext = {
    bcValue.value
  }

  def trainOneBatch(data: Array[LabeledData]): TrainingStat = {

    val localRunStat: TrainingStat = executorContext.conf.get(ConfUtils.ALGO_TYPE) match {
      case "class" =>
        new ClassificationTrainingStat(executorContext.conf.getInt(MLCoreConf.ML_NUM_CLASS))
      case "regression" =>
        new RegressionTrainingStat()
      case "clustering" =>
        new ClusteringTrainingStat()
    }

    val localModel = executorContext.borrowModel(bcConf.value) // those code executor on task

    val graph = localModel.graph

    graph.feedData(data)
    localRunStat.setNumSamples(data.length)
    // note: this step is synchronized
    val pullStart = System.currentTimeMillis()
    if (executorContext.conf.getBoolean(MLCoreConf.ML_IS_DATA_SPARSE)) {
      localModel.pullParams(epoch, graph.placeHolder.getIndices)
    } else {
      localModel.pullParams(epoch)
    }
    val pullFinished = System.currentTimeMillis()
    localRunStat.setPullTime(pullFinished - pullStart)

    val forwardStart = System.currentTimeMillis()
    val avgLoss = graph.calForward()
    graph.predict().foreach { pres => localRunStat.add(pres) }
    localRunStat.setAvgLoss(avgLoss)
    val forwardFinished = System.currentTimeMillis()
    localRunStat.setForwardTime(forwardFinished - forwardStart)

    val backwardStart = System.currentTimeMillis()
    graph.calBackward()
    val backwardFinished = System.currentTimeMillis()
    localRunStat.setBackwardTime(backwardFinished - backwardStart)

    // note: this step is asynchronous
    val pushStart = System.currentTimeMillis()
    localModel.pushGradient(0.1)
    val pushFinished = System.currentTimeMillis()
    localRunStat.setPushTime(pushFinished - pushStart)

    executorContext.returnModel(localModel)

    localRunStat
  }
}
