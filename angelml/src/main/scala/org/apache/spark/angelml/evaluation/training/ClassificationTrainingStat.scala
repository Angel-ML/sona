package org.apache.spark.angelml.evaluation.training

import com.tencent.angel.mlcore.PredictResult
import org.apache.spark.angelml.evaluation.BinaryClassMetrics.BinaryPredictedResult
import org.apache.spark.angelml.evaluation.MultiClassMetrics.MultiPredictedResult
import org.apache.spark.angelml.evaluation._


class ClassificationTrainingStat(val numClasses: Int) extends TrainingStat with Serializable {
  lazy val binStat: BinaryClassMetrics = new BinaryClassMetrics

  lazy val multiStat: MultiClassMetrics = new MultiClassMetrics

  def getSummary: ClassificationSummary = {
    if (numClasses == 2) {
      new BinaryClassificationSummary {
        override val binaryMetrics: BinaryClassMetrics = binStat
        override protected val tp: Double = binStat.getTP
        override protected val fp: Double = binStat.getFP
        override protected val fn: Double = binStat.getFN
        override protected val tn: Double = binStat.getTN
      }
    } else if (numClasses == 3) {
      new MultiClassificationSummary {
        override val multiMetrics: MultiClassMetrics = multiStat
      }
    } else {
      throw new Exception("numClasses Error!")
    }
  }

  override def add(pres: PredictResult): this.type = {
    if (numClasses == 2) {
      binStat.add(BinaryPredictedResult(pres.proba, pres.trueLabel.toInt))
    } else if (numClasses > 2) {
      multiStat.add(MultiPredictedResult(pres.predLabel.toInt, pres.trueLabel.toInt))
    } else {
      throw new Exception("numClasses Error!")
    }

    this
  }

  override def clearStat(): this.type = {
    if (numClasses == 2) {
      binStat.clear()
    } else if (numClasses > 2) {
      multiStat.clear()
    } else {
      throw new Exception("numClasses Error!")
    }

    this
  }

  override protected def mergePart(other: TrainingStat): Unit = {
    val o = other.asInstanceOf[ClassificationTrainingStat]
    assert(numClasses == o.numClasses)

    if (numClasses == 2) {
      binStat.merge(o.binStat)
    } else if (numClasses > 2) {
      multiStat.merge(o.multiStat)
    } else {
      throw new Exception("numClasses Error!")
    }

    super.mergePart(other)
  }

  override def printString(): String = {
    val summary = getSummary

    val insert = if (numClasses == 2) {
      f"ACC=${summary.accuracy}%.3f, " +
        f"AUC=${summary.areaUnderROC}%.3f, " +
        f"Precision=${summary.precision()}%.3f, " +
        f"Recall=${summary.recall()}%.3f, " +
        f"F1Score=${summary.fMeasure(1.0)(1.0)}%.3f"
    } else if (numClasses > 2) {
      f"ACC=${summary.accuracy}%.3f"
    } else {
      ""
    }

    printString(insert)
  }
}
