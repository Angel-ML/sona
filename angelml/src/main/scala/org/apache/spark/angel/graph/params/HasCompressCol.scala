package org.apache.spark.angel.graph.params

import org.apache.spark.angel.ml.param.{Param, Params}

trait HasCompressCol extends Params {
  /**
    * Param for compressCol.
    *
    * @group param
    */
  final val compressCol = new Param[String](this, "compressCol", "name for compress tag column ")

  final def getCompressCol: String = $(compressCol)

  final def setCompressCol(name: String): this.type = set(compressCol, name)

  setDefault(compressCol, "compress")
}
