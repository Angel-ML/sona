package org.apache.spark.angel.graph.params

import org.apache.spark.angel.ml.param.{LongParam, Params}

trait HasApproNodeNum extends Params {

  final val approNodeNum = new LongParam(this, "ApproNodeNum", "ApproNodeNum")

  final def getApproNodeNum: Long = $(approNodeNum)

  setDefault(approNodeNum, -1L)

  final def setApproNodeNum(num: Long): this.type = set(approNodeNum, num)

}
