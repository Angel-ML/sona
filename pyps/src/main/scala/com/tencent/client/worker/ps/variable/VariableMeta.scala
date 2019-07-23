package com.tencent.client.worker.ps.variable

import java.util

import com.tencent.client.common.Meta
import com.tencent.angel.matrix.MatrixContext
import com.tencent.angel.model.MatrixLoadContext
import com.tencent.angel.model.MatrixSaveContext


class VariableMeta(name: String, dtype: String, dim: Int, shape: Array[Long], validIndexNum: Long, val numSlot: Int)
  extends Meta(name, dtype, dim, shape, validIndexNum) {

  override protected val matrixContext: MatrixContext = if (rowType.isDense) {
    val numRows = numSlot + 1
    val numCols = shape.product

    new MatrixContext(name, numRows, numCols, validIndexNum, -1, -1, rowType)
  } else if (rowType.isSparse) {
    assert(dim <= 2 && dim > 0)
    if (dim == 1) {
      val numRows = numSlot + 1
      val numCols = shape(0)
      new MatrixContext(name, numRows, numCols, validIndexNum, -1, -1, rowType)
    } else {
      val numRows = (numSlot + 1) * shape(0).asInstanceOf[Int]
      val numCols = shape(1)
      new MatrixContext(name, numRows, numCols, validIndexNum, -1, -1, rowType)
    }
  } else {
    throw new Exception("cannot create matrix!")
  }

  def this(name: String, dtype: String, dim: Int, shape: Array[Long], numSlot: Int) {
    this(name, dtype, dim, shape, -1, numSlot)
  }

  def this(name: String, dtype: String, dim: Int, shape: Array[Long]) {
    this(name, dtype, dim, shape, -1, 1)
  }

  def this(name: String, dtype: String, numRows: Int, numCols: Long, validIndexNum: Long) {
    this(name, dtype, 2, Array[Long](numRows, numCols), validIndexNum, 1)
  }

  def this(name: String, dtype: String, numRows: Int, numCols: Long) {
    this(name, dtype, numRows, numCols, -1)
  }

  def getMatrixContext: MatrixContext = matrixContext

  def getMatrixSaveContext(path: String, formatClassName: String): MatrixSaveContext = {
    val list = new util.ArrayList[java.lang.Integer]()
    val originRows = matrixContext.getRowNum / (numSlot + 1)
    (0 until originRows).foreach(i => list.add(i))

    new MatrixSaveContext(name, list, formatClassName)
  }

  def getMatrixLoadContext(path: String): MatrixLoadContext = {
    new MatrixLoadContext(name, path)
  }


  override def equals(obj: Any): Boolean = {
    if (super.equals(obj)) {
      this.numSlot == obj.asInstanceOf[VariableMeta].numSlot
    } else {
      false
    }
  }
}
