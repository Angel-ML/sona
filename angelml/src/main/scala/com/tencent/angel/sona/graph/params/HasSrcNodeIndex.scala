package com.tencent.angel.sona.graph.params

import com.tencent.angel.sona.ml.param.{IntParam, Params}

trait HasSrcNodeIndex extends Params {

  final val srcNodeIndex = new IntParam(this, "srcNodeIndex", "index of src node in input")

  final def getSrcNodeIndex(): Int = $(srcNodeIndex)

  setDefault(srcNodeIndex, 0)

  final def setSrcNodeIndex(index: Int): this.type = set(srcNodeIndex, index)

}
