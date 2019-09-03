package com.tencent.angel.sona.graph.params

import com.tencent.angel.sona.ml.param.{Param, Params, StringArrayParam}

trait HasInput extends Params {

  final val input = new Param[String](this, "input", "input")

  final def getInput: String = $(input)

  setDefault(input, null)

  final def setInput(in: String): this.type = set(input, in)

}
