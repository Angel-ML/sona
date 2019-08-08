package org.apache.spark.angel.ml.param

import com.tencent.angel.mlcore.conf.MLCoreConf
import org.apache.spark.angel.ml.param.shared.{HasNumFeatures, HasWeightCol}


trait AngelDataParams extends Params with HasNumFeatures with HasModelSize with HasNumField
  with HasSparseData with HasWeightCol with ParamsHelper {

  def setNumFeatures(value: Long): this.type = setInternal(numFeature, value)

  setDefault(numFeature -> MLCoreConf.DEFAULT_ML_FEATURE_INDEX_RANGE)

  def setModelSize(value:Long): this.type = setInternal(modelSize, value)

  setDefault(modelSize -> MLCoreConf.DEFAULT_ML_MODEL_SIZE)

  def setNumField(value: Int): this.type = setInternal(numField, value)

  setDefault(numField -> MLCoreConf.DEFAULT_ML_FIELD_NUM)

  def setIsSparse(value: Boolean): this.type = setInternal(isSparse, value)

  setDefault(isSparse -> true)
}

trait HasNumField extends Params {
  final val numField: IntParam = new IntParam(this, "numField",
    "the number of field of each instance (> 0)", (value: Int) => value == -1 || value > 0)

  final def getNumField: Int = $(numField)
}


trait HasModelSize extends Params {
  final val modelSize: LongParam = new LongParam(this, "modelSize",
    "the model size must (> 0)", (value: Long) => value == -1 || value > 0)

  final def getModelSize: Long = $(modelSize)
}

trait HasNumClasses extends Params {
  final val numClass: IntParam = new IntParam(this, "numClasses",
    "the number of classes (> 0)", ParamValidators.gt(0))

  final def getNumClass: Int = $(numClass)
}


trait HasSparseData extends Params {
  final val isSparse: BooleanParam = new BooleanParam(this, "isSparse",
    "is the input data sparse ?")

  final def getIsSparse: Boolean = $(isSparse)
}
