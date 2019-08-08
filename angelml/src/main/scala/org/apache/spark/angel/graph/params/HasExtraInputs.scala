package org.apache.spark.angel.graph.params

import org.apache.spark.angel.ml.param.{Params, StringArrayParam}

trait HasExtraInputs extends Params {

  final val extraInputs = new StringArrayParam(this, "extraInput", "extraInput")

  final def getExtraInputs: Array[String] = $(extraInputs)

  setDefault(extraInputs, Array.empty[String])

  final def setExtraInputs(arr: Array[String]): this.type = set(extraInputs, arr)

}
