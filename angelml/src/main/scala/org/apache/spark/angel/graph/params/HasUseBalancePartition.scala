package org.apache.spark.angel.graph.params

import org.apache.spark.angel.ml.param.{BooleanParam, Params}

trait HasUseBalancePartition extends Params {

  final val useBalancePartition = new BooleanParam(this, "useBalancePartition", "useBalancePartition")

  final def getUseBalancePartition: Boolean = $(useBalancePartition)

  setDefault(useBalancePartition, false)

  final def setUseBalancePartition(flag: Boolean): this.type = set(useBalancePartition, flag)

}
