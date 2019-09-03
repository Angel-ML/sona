package com.tencent.angel.sona.ml.automl

object TunedParams {

  lazy val Params = Array(
    "maxIter",
    "learningRate",
    "numBatch",
    "decayAlpha",
    "decayBeta",
    "decayIntervals"
  )

  def exists(param: String): Boolean = Params.contains(param)
}
