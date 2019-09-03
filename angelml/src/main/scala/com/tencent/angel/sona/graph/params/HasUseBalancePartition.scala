package com.tencent.angel.sona.graph.params

import com.tencent.angel.sona.ml.param.{BooleanParam, Params}

trait HasUseBalancePartition extends Params {

  final val useBalancePartition = new BooleanParam(this, "useBalancePartition", "useBalancePartition")

  final def getUseBalancePartition: Boolean = $(useBalancePartition)

  setDefault(useBalancePartition, false)

  final def setUseBalancePartition(flag: Boolean): this.type = set(useBalancePartition, flag)

}
