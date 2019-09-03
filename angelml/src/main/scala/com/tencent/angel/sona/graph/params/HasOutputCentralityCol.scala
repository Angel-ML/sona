package com.tencent.angel.sona.graph.params

import com.tencent.angel.sona.ml.param.{Param, Params}

trait HasOutputCentralityCol extends Params {
  /**
    * Param for name of output centrality.
    *
    * @group param
    */
  final val outputCentralityCol = new Param[String](this, "outputCentralityCol",
    "name for output centrality column")

  /** @group getParam */
  final def getOutputCentralityCol: String = $(outputCentralityCol)

  setDefault(outputCentralityCol, "centrality")

  /** @group setParam */
  def setOutputCentralityCol(name: String): this.type = set(outputCentralityCol, name)
}
