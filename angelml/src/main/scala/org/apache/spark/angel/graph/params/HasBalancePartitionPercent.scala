package org.apache.spark.angel.graph.params

import org.apache.spark.angel.ml.param.{FloatParam, Params}

trait HasBalancePartitionPercent extends Params {

  final val balancePartitionPercent = new FloatParam(this, "balancePartitionPercent", "balancePartitionPercent")

  final def getBalancePartitionPercent: Float = $(balancePartitionPercent)

  setDefault(balancePartitionPercent, 0.7f)

  final def setBalancePartitionPercent(percent: Float): this.type = set(balancePartitionPercent, percent)

}
