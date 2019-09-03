package com.tencent.angel.sona.graph.params

import com.tencent.angel.sona.ml.param.{IntParam, Params}

trait HasBufferSize extends Params {
  /**
    * Param for buffer size.
    *
    * @group param
    */
  final val bufferSize = new IntParam(this, "bufferSize", "bufferSize")

  /** @group getParam */
  final def getBufferSize: Int = $(bufferSize)

  setDefault(bufferSize, 1000000)

  /** @group setParam */
  final def setBufferSize(size: Int): this.type = set(bufferSize, size)
}
