package org.apache.spark.angelml.param

import com.tencent.angel.ml.core.conf.MLCoreConf
import org.apache.spark.angelml.param.shared.HasMaxIter


trait AngelOptParams extends Params with HasMaxIter with HasLearningRate
  with HasNumBatch with HasDecayConf with HasIncTrain with ParamsHelper {

  def setMaxIter(value: Int): this.type = setInternal(maxIter, value)

  setDefault(maxIter -> MLCoreConf.DEFAULT_ML_EPOCH_NUM)

  def setLearningRate(value: Double): this.type = setInternal(learningRate, value)

  setDefault(learningRate -> MLCoreConf.DEFAULT_ML_LEARN_RATE)

  def setNumBatch(value: Int): this.type = setInternal(numBatch, value)

  setDefault(numBatch -> MLCoreConf.DEFAULT_ML_NUM_UPDATE_PER_EPOCH)

  def setDecayClass(value: String): this.type = setInternal(decayClass, value)

  setDefault(decayClass -> MLCoreConf.DEFAULT_ML_OPT_DECAY_CLASS_NAME)

  def setDecayAlpha(value: Double): this.type = setInternal(decayAlpha, value)

  setDefault(decayAlpha -> MLCoreConf.DEFAULT_ML_OPT_DECAY_ALPHA)

  def setDecayBeta(value: Double): this.type = setInternal(decayBeta, value)

  setDefault(decayBeta -> MLCoreConf.DEFAULT_ML_OPT_DECAY_BETA)

  def setDecayIntervals(value: Int): this.type = setInternal(decayIntervals, value)

  setDefault(decayIntervals -> MLCoreConf.DEFAULT_ML_OPT_DECAY_INTERVALS)

  def setDecayOnBatch(value: Boolean): this.type = setInternal(decayOnBatch, value)

  setDefault(decayOnBatch -> MLCoreConf.DEFAULT_ML_OPT_DECAY_ON_BATCH)

  setDefault(incTrain, false)

  def setInitModelPath(value: String): this.type = {
    set(incTrain, true)
    setInternal(initModelPath, value)
  }
}


trait HasLearningRate extends Params {
  final val learningRate: DoubleParam = new DoubleParam(this, "learningRate",
    "learning rate (> 0)", ParamValidators.gt(0))

  final def getLearningRate: Double = $(learningRate)
}


trait HasNumBatch extends Params {
  final val numBatch: IntParam = new IntParam(this, "numBatch",
    "number of batches in each epock (> 0)", ParamValidators.gt(0))

  final def getNumBatch: Int = $(numBatch)
}


trait HasDecayConf extends Params {
  final val decayClass: Param[String] = new Param[String](this, "decayClass",
    "the learning rate decay class name", (value: String) => value != null && value.nonEmpty)

  final val decayAlpha: DoubleParam = new DoubleParam(this, "decayAlpha",
    "the first params of decay", ParamValidators.gt(0))

  final val decayBeta: DoubleParam = new DoubleParam(this, "decayBeta",
    "the second params of decay", ParamValidators.gt(0))

  final val decayIntervals: IntParam = new IntParam(this, "decayIntervals",
    "the decay intervals", ParamValidators.gt(0))

  final val decayOnBatch: BooleanParam = new BooleanParam(this, "decayOnBatch",
    "is the decay on batch or epoch ?")

  final def getDecayClass: String = $(decayClass)

  final def getDecayAlpha: Double = $(decayAlpha)

  final def getDecayBeta: Double = $(decayBeta)

  final def getDecayIntervals: Int = $(decayIntervals)

  final def getDecayOnBatch: Boolean = $(decayOnBatch)
}


trait HasIncTrain extends Params {
  final val incTrain: BooleanParam = new BooleanParam(this, "incTrain",
    "is increase training ?")

  final val initModelPath: Param[String] = new Param[String](this, "initModelPath",
    "the model file path", (value: String) => value != null && value.nonEmpty)

  final def getIncTrain: Boolean = $(incTrain)

  final def getInitModelPath: String = $(initModelPath)
}
