package com.tencent.angel.ps.variable


import java.util.concurrent.Future

import com.tencent.angel.apiserver.HandlerId
import com.tencent.angel.ml.servingmath2.VFactory
import com.tencent.angel.ml.servingmath2.matrix._
import com.tencent.angel.ml.servingmath2.vector._
import com.tencent.angel.ps.common.State
import com.tencent.angel.ps.tensor.TensorLike
import com.tencent.angel.ps.updater.Optimizer

@HandlerId(2)
class Variable(name: String, dim: Int, shape: Array[Long], dtype: String, validIndexNum: Long, val updaterParams: Map[String, String],
               initializer: Initializer = new NormalInitializer(0.0, 1e-6))
  extends TensorLike(name, dim, shape, dtype, validIndexNum, initializer) {

  protected lazy val numSlot: Int = updaterParams.getOrElse("numSlot", "0").toInt
  override protected val meta: VariableMeta = new VariableMeta(name, dtype, dim, shape, validIndexNum, numSlot)
  protected val updater: Updater = Optimizer.get(updaterParams)

  override def getMeta: VariableMeta = meta

  protected def doPull(epoch: Int, indices: Vector): Array[Vector] = {
    val originRows = meta.getMatrixContext.getRowNum / (meta.getNumSlot + 1)
    val rowIds = (0 until originRows).toArray

    if (epoch == 0 && indices != null) {
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
  }

  protected def doPush(grad: Matrix, alpha: Double): Unit = {
    val matrixId = matClient.getMatrixId
    val originRows = meta.getMatrixContext.getRowNum / (meta.getNumSlot + 1)
    assert(grad.getNumRows == originRows)

    grad match {
      case gblas: BlasDoubleMatrix =>
        val row = VFactory.denseDoubleVector(gblas.getData)
        row.imul(alpha)
        row.setMatrixId(matrixId)
        row.setRowId(meta.getNumSlot)
        matClient.update(row)
      case gblas: BlasFloatMatrix =>
        val row = VFactory.denseFloatVector(gblas.getData)
        row.imul(alpha)
        row.setMatrixId(matrixId)
        row.setRowId(meta.getNumSlot)
        matClient.update(row)
      case grbase: RowBasedMatrix[_] =>
        val rowIds = (originRows * meta.getNumSlot until meta.getMatrixContext.getRowNum).toArray
        val rows = rowIds.map { rowId =>
          val row = grbase.getRow(rowId - originRows * meta.getNumSlot)
          row.imul(alpha)
          row.setMatrixId(matrixId)
          row.setRowId(rowId)

          row
        }

        matClient.update(rowIds, rows)
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