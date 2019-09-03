package com.tencent.angel.sona.graph.params

import com.tencent.angel.sona.ml.param.{Param, Params}

trait HasDelimiter extends Params {

  final val delimiter = new Param[String](this, "delimiter", "delimiter of input")

  final def getDelimiter: String = $(delimiter)

  setDefault(delimiter, " ")

  final def setDelimiter(sep: String): this.type = set(delimiter, sep)

}
