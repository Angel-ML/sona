package org.apache.spark.angel.graph.params

import org.apache.spark.angel.ml.param.{Param, Params, StringArrayParam}

trait HasInput extends Params {

  final val input = new Param[String](this, "input", "input")

  final def getInput: String = $(input)

  setDefault(input, null)

  final def setInput(in: String): this.type = set(input, in)

}
