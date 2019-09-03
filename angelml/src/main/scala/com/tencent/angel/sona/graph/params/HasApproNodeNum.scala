package com.tencent.angel.sona.graph.params

import com.tencent.angel.sona.ml.param.{LongParam, Params}

trait HasApproNodeNum extends Params {

  final val approNodeNum = new LongParam(this, "ApproNodeNum", "ApproNodeNum")

  final def getApproNodeNum: Long = $(approNodeNum)

  setDefault(approNodeNum, -1L)

  final def setApproNodeNum(num: Long): this.type = set(approNodeNum, num)

}
