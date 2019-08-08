package org.apache.spark.angel.graph.params

import org.apache.spark.angel.ml.param.{IntParam, Params}

trait HasDstNodeIndex extends Params {

  final val dstNodeIndex = new IntParam(this, "dstNodeIndex", "index of dst node in input")

  final def getDstNodeIndex(): Int = $(dstNodeIndex)

  setDefault(dstNodeIndex, 1)

  final def setDstNodeIndex(index: Int): this.type = set(dstNodeIndex, index)

}
