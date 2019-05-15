package org.apache.spark.angelml.param

import com.tencent.angel.ml.core.conf.MLCoreConf
import org.apache.spark.angelml.param.shared.{HasNumFeatures, HasWeightCol}


trait AngelDataParams extends Params with HasNumFeatures with HasNumField
  with HasSparseData with HasWeightCol with ParamsHelper {

  def setNumFeature(value: Long): this.type = setInternal(numFeatures, value)

  setDefault(numFeatures -> MLCoreConf.DEFAULT_ML_FEATURE_INDEX_RANGE)

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


trait HasNumClasses extends Params {
  final val numClasses: IntParam = new IntParam(this, "numClasses",
    "the number of classes (> 0)", ParamValidators.gt(0))

  final def getNumClasses: Int = $(numClasses)
}


trait HasSparseData extends Params {
  final val isSparse: BooleanParam = new BooleanParam(this, "isSparse",
    "is the input data sparse ?")

  final def getIsSparse: Boolean = $(isSparse)
}
