package org.apache.spark.angel.graph.params

import org.apache.spark.angel.ml.param.{IntParam, Params}

trait HasWeightIndex extends Params {

  final val weightIndex = new IntParam(this, "weightIndex", "index of weight in input")

  final def getWeightIndex(): Int = $(weightIndex)

  setDefault(weightIndex, 2)

  final def setWeightIndex(index: Int): this.type = set(weightIndex, index)

}
