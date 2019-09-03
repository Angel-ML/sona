package com.tencent.angel.sona.graph.params

import com.tencent.angel.sona.ml.param.{IntParam, Params}

trait HasBatchSize extends Params {
  /**
    * Param for batch size.
    *
    * @group param
    */
  final val batchSize = new IntParam(this, "batchSize", "batchSize")

  /** @group getParam */
  final def getBatchSize: Int = $(batchSize)

  setDefault(batchSize, 10000)

  /** @group setParam */
  final def setBatchSize(size: Int): this.type = set(batchSize, size)
}
