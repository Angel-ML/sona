package com.tencent.angel.sona.graph.params

import com.tencent.angel.sona.ml.param.{Param, Params}

trait HasOutputPageRankCol extends Params {

  /**
    * Param for name of output pagerank value
    *
    * #group param
    */

  final val outputPageRankCol = new Param[String](this, "outputPageRankCol",
    "name for output pagerank column")

  /** @group getParam */
  final def getOutputPageRankCol: String = ${outputPageRankCol}

  setDefault(outputPageRankCol, "pagerank")

  /** @group setParam */
  final def setOutputPageRankCol(name: String): this.type = set(outputPageRankCol, name)
}