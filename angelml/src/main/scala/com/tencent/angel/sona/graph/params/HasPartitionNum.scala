package com.tencent.angel.sona.graph.params

import com.tencent.angel.sona.ml.param.{IntParam, Params}

trait HasPartitionNum extends Params {
  /**
    * Param for partitionNum.
    *
    * @group param
    */
  final val partitionNum = new IntParam(this, "partitionNum", "num of partition for rdd")

  /** @group getParam */
  final def getPartitionNum: Int = $(partitionNum)

  final def setPartitionNum(num: Int): this.type = set(partitionNum, num)

}
