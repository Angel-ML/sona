package com.tencent.client.ps.variable

import java.util.concurrent.Future

import com.tencent.client.common.Utils
import com.tencent.angel.ml.servingmath2.VFactory
import com.tencent.angel.ml.servingmath2.matrix._
import com.tencent.angel.ml.servingmath2.vector._
import com.tencent.client.ps.common.State
import com.tencent.client.ps.tensor.TensorLike


class Variable(name: String, dim: Int, shape: Array[Long], dtype: String, validIndexNum: Long, updater: Updater, initializer: Initializer)
  extends TensorLike(name, dim, shape, dtype, validIndexNum, initializer) {

  protected lazy val numSlot: Int = updater.numSlot
  override protected val meta: VariableMeta = new VariableMeta(name, dtype, dim, shape, validIndexNum, numSlot)


  override def getMeta: VariableMeta = meta

  protected def doPull(epoch: Int, idxs: Matrix): Matrix = {
    val indices = idxs.getRow(0)
    val originRows = meta.getMatrixContext.getRowNum / (meta.numSlot + 1)
    val rowIds = (0 until originRows).toArray

    val pulled = if (epoch == 0 && indices != null) {
      val func = initializer.getInitFunc(matClient.getMatrixId, meta)
      indices match {
        case v: IntIntVector if v.isDense =>
          matClient.initAndGet(rowIds, v.getStorage.getValues, func)
        case v: IntDummyVector =>
          matClient.initAndGet(rowIds, v.getIndices, func)
        case v: IntLongVector if v.isDense =>
          matClient.initAndGet(rowIds, v.getStorage.getValues, func)
        case v: LongDummyVector =>
          matClient.initAndGet(rowIds, v.getIndices, func)
      }
    } else {
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

    Utils.vectorArray2Matrix(pulled)
  }

  protected def doPush(grad: Matrix, alpha: Double): Unit = {
    val matrixId = matClient.getMatrixId
    val originRows = meta.getMatrixContext.getRowNum / (meta.numSlot + 1)
    assert(grad.getNumRows == originRows)

    grad match {
      case gblas: BlasDoubleMatrix =>
        val row = VFactory.denseDoubleVector(gblas.getData)
        row.imul(alpha)
        row.setMatrixId(matrixId)
        row.setRowId(meta.numSlot)
        matClient.update(row)
      case gblas: BlasFloatMatrix =>
        val row = VFactory.denseFloatVector(gblas.getData)
        row.imul(alpha)
        row.setMatrixId(matrixId)
        row.setRowId(meta.numSlot)
        matClient.update(row)
      case grbase: RowBasedMatrix[_] =>
        val rowIds = (originRows * meta.numSlot until meta.getMatrixContext.getRowNum).toArray
        val rows = rowIds.map { rowId =>
          val row = grbase.getRow(rowId - originRows * meta.numSlot)
          row.imul(alpha)
          row.setMatrixId(matrixId)
          row.setRowId(rowId)

          row
        }

        matClient.update(rowIds, rows.asInstanceOf[Array[Vector]])
    }
  }

  def update[T](epoch: Int, batchSize: Int): Future[T] = {
    writeLock.lock()

    try {
      assert(state != State.New && state != State.Expired)

      if (numSlot > 0) {
        updater.update[T](this, epoch, batchSize)
      } else {
        null.asInstanceOf[Future[T]]
      }
    } finally {
      writeLock.unlock()
    }
  }
}