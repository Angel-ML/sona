package com.tencent.angel.sona.graph.params

import com.tencent.angel.sona.ml.param.{BooleanParam, Params}

trait HasUseEstimatePartition extends Params {

  final val useEstimatePartition = new BooleanParam(this, "useEstimatePartition", "useEstimatePartition")

  final def getUseEstimatePartition: Boolean = $(useEstimatePartition)

  setDefault(useEstimatePartition, false)

  final def setUseEstimatePartition(flag: Boolean): this.type = set(useEstimatePartition, flag)

}
