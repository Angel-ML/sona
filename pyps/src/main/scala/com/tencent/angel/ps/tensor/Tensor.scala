package com.tencent.angel.ps.tensor

import com.tencent.angel.apiserver.HandlerId
import com.tencent.angel.ml.servingmath2.VFactory
import com.tencent.angel.ml.servingmath2.matrix._
import com.tencent.angel.ml.servingmath2.vector._
import com.tencent.angel.ps.variable.{Initializer, NormalInitializer}


class Tensor(name: String, dim: Int, shape: Array[Long], dtype: String, validIndexNum: Long,
             initializer: Initializer = new NormalInitializer(0.0, 1e-6))
  extends TensorLike(name, dim, shape, dtype, validIndexNum, initializer) {

  override protected val meta: TensorMeta = new TensorMeta(name, dtype, dim, shape, validIndexNum)

  override def getMeta: TensorMeta = meta

  protected def doPull(epoch: Int, indices: Vector): Array[Vector] = {
    val rowIds = (0 until meta.getMatrixContext.getRowNum).toArray

    if (indices != null) {
      indices match {
        case v: IntIntVector if v.isDense =>
          matClient.get(rowIds, v.getStorage.getValues)
        case v: IntDummyVector =>
          matClient.get(rowIds, v.getIndices)
        case v: IntLongVector if v.isDense =>
          matClient.get(rowIds, v.getStorage.getValues)
        case v: LongDummyVector =>
          matClient.get(rowIds, v.getIndices)
      }
    } else {
      matClient.getRows(rowIds)
    }
  }

  protected def doPush(data: Matrix, alpha: Double): Unit = {
    val matrixId = matClient.getMatrixId

    data match {
      case dblas: BlasDoubleMatrix =>
        val row = VFactory.denseDoubleVector(dblas.getData)
        row.imul(alpha)
        row.setMatrixId(matrixId)
        row.setRowId(0)
        matClient.update(row)
      case fblas: BlasFloatMatrix =>
        val row = VFactory.denseFloatVector(fblas.getData)
        row.imul(alpha)
        row.setMatrixId(matrixId)
        row.setRowId(0)
        matClient.update(row)
      case rbase: RowBasedMatrix[_] =>
        val rowIds = (0 until meta.getMatrixContext.getRowNum).toArray
        val rows = rowIds.map { rowId =>
          val row = rbase.getRow(rowId)
          row.imul(alpha)
          row.setMatrixId(matrixId)
          row.setRowId(rowId)

          row
        }

        matClient.update(rowIds, rows)
    }
  }

}
