package org.apache.spark.angel.graph.params

import org.apache.spark.angel.ml.param.{BooleanParam, Params}

trait HasIsCompressed extends Params {
  /**
    * Param for isCompressed.
    *
    * @group param
    */
  final val isCompressed = new BooleanParam(this, "isCompressed", "is compressed edge or not")

  final def getIsCompressed : Boolean = $(isCompressed)

  setDefault(isCompressed, false)

  final def setIsCompressed (bool: Boolean): this.type = set(isCompressed, bool)
}
