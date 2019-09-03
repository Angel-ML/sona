package com.tencent.angel.sona.graph.params

import com.tencent.angel.sona.ml.param.{Param, Params}

trait HasWeightCol extends Params {
  /**
    * Param for weightCol.
    *
    * @group param
    */
  final val weightCol = new Param[String](this, "weightCol", "name for weight column ")

  /** @group getParam */
  final def getWeightCol: String = $(weightCol)

  final def setWeightCol(name: String): this.type = set(weightCol, name)

  setDefault(weightCol, "weight")
}
