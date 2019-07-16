package com.tencent.angel.ps.common

import com.tencent.angel.ml.servingmath2.matrix.Matrix

class VersionedMatrix(val matId: Int, val epoch: Int, val batch: Int, val mat: Matrix) {
  def >(other: VersionedMatrix): Boolean = {
    assert(matId == other.matId)

    true
  }
}
