package com.tencent.client.worker.ps.tensor

import com.tencent.angel.matrix.MatrixContext
import com.tencent.angel.model.MatrixLoadContext
import com.tencent.angel.model.MatrixSaveContext
import com.tencent.client.common.Meta


class TensorMeta(name: String, dtype: String, dim: Int, shape: Array[Long], validIndexNum: Long)
  extends Meta(name, dtype, dim, shape, validIndexNum) {

  override protected val matrixContext: MatrixContext = if (rowType.isDense) {
    val numCols = shape.product

    new MatrixContext(name, 1, numCols, validIndexNum, -1, -1, rowType)
  } else if (rowType.isSparse) {
    assert(dim <= 2 && dim > 0)
    if (dim == 1) {
      val numCols = shape(0)
      new MatrixContext(name, 1, numCols, validIndexNum, -1, -1, rowType)
    } else { // dim == 2
      new MatrixContext(name, shape(0).toInt, shape(1), validIndexNum, -1, -1, rowType)
    }
  } else {
    throw new Exception("cannot create matrix!")
  }

  def this(name: String, dtype: String, dim: Int, shape: Array[Long]) {
    this(name, dtype, dim, shape, -1)
  }

  def this(name: String, dtype: String, numRows: Int, numCols: Long, validIndexNum: Long) {
    this(name, dtype, 2, Array[Long](numRows, numCols), validIndexNum)
  }

  def this(name: String, dtype: String, numRows: Int, numCols: Long) {
    this(name, dtype, numRows, numRows, -1)
  }

  def getMatrixContext: MatrixContext = {
    matrixContext
  }

  def getMatrixSaveContext(path: String, formatClassName: String): MatrixSaveContext = {
    new MatrixSaveContext(name, formatClassName)
  }

  def getMatrixLoadContext(path: String): MatrixLoadContext = {
    new MatrixLoadContext(name, path)
  }


}
