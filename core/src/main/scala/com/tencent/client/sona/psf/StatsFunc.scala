package com.tencent.client.sona.psf

import com.tencent.angel.matrix.psf.aggr.enhance.UnaryAggrFunc
import com.tencent.angel.ps.storage.vector.ServerRow

class StatsFunc(matrixId: Int) extends UnaryAggrFunc(matrixId, 0) {

  def this() = { this(-1) }

  override protected def mergeInit: Double = 0.0

  override protected def mergeOp(a: Double, b: Double): Double = {
    a + b
  }

  override protected def processRow(row: ServerRow): Double = {
    row.getSplit.getSize
  }
}
