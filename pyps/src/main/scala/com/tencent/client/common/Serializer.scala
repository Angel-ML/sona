package com.tencent.angel.common

import java.nio.{ByteBuffer, ByteOrder}

import com.tencent.angel.ml.servingmath2.MFactory
import com.tencent.angel.ml.servingmath2.matrix._
import com.tencent.angel.ml.servingmath2.vector._


object Serializer {
  private def isDense(mat: Matrix): Boolean = {
    mat.getRow(0).isDense
  }

  private def isSparse(mat: Matrix): Boolean = {
    mat.getRow(0).isSparse
  }

  private def isSorted(mat: Matrix): Boolean = {
    mat.getRow(0).isSorted
  }

  def getDataHeadFromMatrix(mat: Matrix, meta: Meta): DataHead = {
    mat match {
      case m: BlasDoubleMatrix =>
        val data = m.getData
        new DataHead(-1, meta.shape.length, meta.shape, data.length, DataHead.DT_DOUBLE, data.length * 8)
      case m: BlasFloatMatrix =>
        val data = m.getData
        new DataHead(-1, meta.shape.length, meta.shape, data.length, DataHead.DT_FLOAT, data.length * 4)
      case m: RowBasedMatrix[_] =>
        val rowIds = m.getRows.map { vec: Vector =>
          vec.getRowId.toLong
        }

        var idx = -1
        val isRowDense = rowIds.forall { ridx =>
          idx += 1
          ridx == idx
        }

        val isColDense = (0 until mat.getNumRows).forall { i =>
          mat.getRow(i) match {
            case v: Vector => v.isDense
          }
        }

        val (sparseDim: Int, denseDim: Int, nnz: Int) = if (isRowDense && isColDense) {
          (-1, meta.shape.length, meta.shape.product.toInt)
        } else if (!isRowDense && isColDense) {
          (1, meta.shape.length - 1, m.getNumRows)
        } else if (!isRowDense && !isColDense && meta.shape.length == 2) {
          var numEles = 0
          (0 until m.getNumRows).foreach { i =>
            numEles += m.getRow(i).getSize.toInt
          }

          (2, 0, numEles)
        } else {
          throw new Exception("data not supported!")
        }

        val dtype: Int = mat.getRow(0) match {
          case _: DoubleVector => DataHead.DT_DOUBLE
          case _: FloatVector => DataHead.DT_FLOAT
          case _: LongVector => DataHead.DT_LONG
          case _: IntVector => DataHead.DT_INT
          case _: Vector => DataHead.DT_FLOAT
        }

        val length = dtype match {
          case dt if dt == DataHead.DT_INT =>
            if (isRowDense && isColDense) {
              nnz * 4
            } else if (!isRowDense && isColDense) {
              mat.getNumRows * 8 + nnz * 4
            } else {
              nnz * 20
            }
          case dt if dt == DataHead.DT_LONG =>
            if (isRowDense && isColDense) {
              nnz * 8
            } else if (!isRowDense && isColDense) {
              mat.getNumRows * 8 + nnz * 8
            } else {
              nnz * 24
            }
          case dt if dt == DataHead.DT_FLOAT =>
            if (isRowDense && isColDense) {
              nnz * 4
            } else if (!isRowDense && isColDense) {
              mat.getNumRows * 8 + nnz * 4
            } else {
              nnz * 20
            }
          case dt if dt == DataHead.DT_DOUBLE =>
            if (isRowDense && isColDense) {
              nnz * 8
            } else if (!isRowDense && isColDense) {
              mat.getNumRows * 8 + nnz * 8
            } else {
              nnz * 24
            }
        }

        new DataHead(sparseDim, denseDim, meta.shape, nnz, dtype, length)
      case _ => throw new Exception("matrix type is not supported!")
    }
  }

  def putMatrix(buf: ByteBuffer, mat: Matrix, meta: Meta, head: DataHead): Unit = {
    // DataHead(dim: Int, rows: Long, cols: Long, nnz: Int, dtype: Int, storageFlag: Int, length: Int)
    val oldOrder = buf.order()
    buf.order(ByteOrder.LITTLE_ENDIAN)
    val dataHead = if (head == null) {
      getDataHeadFromMatrix(mat, meta)
    } else {
      head
    }
    dataHead.put2Buf(buf)

    mat match {
      case m: BlasMatrix => putBlasMatrix(buf, m, dataHead)
      case m: RowBasedMatrix[_] if isDense(mat) => putDenseRBMatrix(buf, m, dataHead)
      case m: RowBasedMatrix[_] if isSparse(mat) || isSorted(mat) => putSparseRBMatrix(buf, m, dataHead)
      case _ => throw new Exception("matrix type is not supported!")
    }

    buf.order(oldOrder)
  }

  private def putBlasMatrix(buf: ByteBuffer, mat: BlasMatrix, dataHead: DataHead): Unit = {
    mat match {
      case m: BlasDoubleMatrix =>
        val data = m.getData
        data.foreach(value => buf.putDouble(value))
      case m: BlasFloatMatrix =>
        val data = m.getData
        data.foreach(value => buf.putFloat(value))
    }
  }

  private def putDenseRBMatrix(buf: ByteBuffer, mat: RowBasedMatrix[_], dataHead: DataHead): Unit = {
    if (dataHead.sparseDim == 1) {
      (0 until mat.getNumRows).foreach { i =>
        mat.getRow(i) match {
          case v: Vector => buf.putLong(v.getRowId.toLong)
        }
      }
    }

    (0 until mat.getNumRows).foreach { i =>
      mat.getRow(i) match {
        case v: IntDoubleVector =>
          v.getStorage.getValues.foreach(value => buf.putDouble(value))
        case v: IntFloatVector =>
          v.getStorage.getValues.foreach(value => buf.putFloat(value))
        case v: IntLongVector =>
          v.getStorage.getValues.foreach(value => buf.putLong(value))
        case v: IntIntVector =>
          v.getStorage.getValues.foreach(value => buf.putInt(value))
      }
    }
  }

  private def putSparseVector(vec: Vector, rowBuf: ByteBuffer, colBuf: ByteBuffer, valBuf: ByteBuffer): Unit = {
    val rowIdx: Int = vec.getRowId

    vec match {
      case v: IntDoubleVector =>
        val iter = v.getStorage.entryIterator()
        while (iter.hasNext) {
          val entry = iter.next()
          if (rowBuf != null) {
            rowBuf.putLong(rowIdx.toLong)
          }
          colBuf.putLong(entry.getIntKey.toLong)
          valBuf.putDouble(entry.getDoubleValue)
        }
      case v: IntFloatVector =>
        val iter = v.getStorage.entryIterator()
        while (iter.hasNext) {
          val entry = iter.next()
          if (rowBuf != null) {
            rowBuf.putLong(rowIdx.toLong)
          }
          colBuf.putLong(entry.getIntKey.toLong)
          valBuf.putFloat(entry.getFloatValue)
        }
      case v: IntLongVector =>
        val iter = v.getStorage.entryIterator()
        while (iter.hasNext) {
          val entry = iter.next()
          if (rowBuf != null) {
            rowBuf.putLong(rowIdx.toLong)
          }
          colBuf.putLong(entry.getIntKey.toLong)
          valBuf.putLong(entry.getLongValue)
        }
      case v: IntIntVector =>
        val iter = v.getStorage.entryIterator()
        while (iter.hasNext) {
          val entry = iter.next()
          if (rowBuf != null) {
            rowBuf.putLong(rowIdx.toLong)
          }
          colBuf.putLong(entry.getIntKey.toLong)
          valBuf.putInt(entry.getIntValue)
        }
      case v: LongDoubleVector =>
        val iter = v.getStorage.entryIterator()
        while (iter.hasNext) {
          val entry = iter.next()
          if (rowBuf != null) {
            rowBuf.putLong(rowIdx.toLong)
          }
          colBuf.putLong(entry.getLongKey)
          valBuf.putDouble(entry.getDoubleValue)
        }
      case v: LongFloatVector =>
        val iter = v.getStorage.entryIterator()
        while (iter.hasNext) {
          val entry = iter.next()
          if (rowBuf != null) {
            rowBuf.putLong(rowIdx.toLong)
          }
          colBuf.putLong(entry.getLongKey)
          valBuf.putFloat(entry.getFloatValue)
        }
      case v: LongLongVector =>
        val iter = v.getStorage.entryIterator()
        while (iter.hasNext) {
          val entry = iter.next()
          if (rowBuf != null) {
            rowBuf.putLong(rowIdx.toLong)
          }
          colBuf.putLong(entry.getLongKey)
          valBuf.putLong(entry.getLongValue)
        }
      case v: LongIntVector =>
        val iter = v.getStorage.entryIterator()
        while (iter.hasNext) {
          val entry = iter.next()
          if (rowBuf != null) {
            rowBuf.putLong(rowIdx.toLong)
          }
          colBuf.putLong(entry.getLongKey)
          valBuf.putInt(entry.getIntValue)
        }
    }
  }

  private def putSortedVector(vec: Vector, rowBuf: ByteBuffer, colBuf: ByteBuffer, valBuf: ByteBuffer): Unit = {
    val rowIdx = vec.getRowId
    vec match {
      case v: IntDoubleVector =>
        val indices = v.getStorage.getIndices
        val values = v.getStorage.getValues
        indices.foreach { idx =>
          if (rowBuf != null) {
            rowBuf.putLong(rowIdx.toLong)
          }
          colBuf.putLong(idx.toLong)
        }

        values.foreach(value => valBuf.putDouble(value))
      case v: IntFloatVector =>
        val indices = v.getStorage.getIndices
        val values = v.getStorage.getValues
        indices.foreach { idx =>
          if (rowBuf != null) {
            rowBuf.putLong(rowIdx.toLong)
          }
          colBuf.putLong(idx.toLong)
        }

        values.foreach(value => valBuf.putFloat(value))
      case v: IntLongVector =>
        val indices = v.getStorage.getIndices
        val values = v.getStorage.getValues
        indices.foreach { idx =>
          if (rowBuf != null) {
            rowBuf.putLong(rowIdx.toLong)
          }
          colBuf.putLong(idx.toLong)
        }

        values.foreach(value => valBuf.putLong(value))
      case v: IntIntVector =>
        val indices = v.getStorage.getIndices
        val values = v.getStorage.getValues
        indices.foreach { idx =>
          if (rowBuf != null) {
            rowBuf.putLong(rowIdx.toLong)
          }
          colBuf.putLong(idx.toLong)
        }

        values.foreach(value => valBuf.putInt(value))
      case v: LongDoubleVector =>
        val indices = v.getStorage.getIndices
        val values = v.getStorage.getValues
        indices.foreach { idx =>
          if (rowBuf != null) {
            rowBuf.putLong(rowIdx.toLong)
          }
          colBuf.putLong(idx)
        }

        values.foreach(value => valBuf.putDouble(value))
      case v: LongFloatVector =>
        val indices = v.getStorage.getIndices
        val values = v.getStorage.getValues
        indices.foreach { idx =>
          if (rowBuf != null) {
            rowBuf.putLong(rowIdx.toLong)
          }
          colBuf.putLong(idx)
        }

        values.foreach(value => valBuf.putFloat(value))
      case v: LongLongVector =>
        val indices = v.getStorage.getIndices
        val values = v.getStorage.getValues
        indices.foreach { idx =>
          if (rowBuf != null) {
            rowBuf.putLong(rowIdx.toLong)
          }
          colBuf.putLong(idx)
        }

        values.foreach(value => valBuf.putLong(value))
      case v: LongIntVector =>
        val indices = v.getStorage.getIndices
        val values = v.getStorage.getValues
        indices.foreach { idx =>
          if (rowBuf != null) {
            rowBuf.putLong(rowIdx.toLong)
          }
          colBuf.putLong(idx)
        }

        values.foreach(value => valBuf.putInt(value))
    }
  }

  private def putSparseRBMatrix(buf: ByteBuffer, mat: RowBasedMatrix[_], dataHead: DataHead): Unit = {
    val bytes = buf.array()
    var start = buf.position
    val rowBuf = ByteBuffer.wrap(bytes, start, dataHead.nnz * 8)
    rowBuf.order(ByteOrder.LITTLE_ENDIAN)
    start += dataHead.nnz * 8

    val colBuf = ByteBuffer.wrap(bytes, start, dataHead.nnz * 8)
    colBuf.order(ByteOrder.LITTLE_ENDIAN)
    start += dataHead.nnz * 8

    val valBuf = ByteBuffer.wrap(bytes, start, bytes.length - start)
    valBuf.order(ByteOrder.LITTLE_ENDIAN)

    (0 until mat.getNumRows).foreach { rowIdx =>
      mat.getRow(rowIdx) match {
        case vec: Vector if vec.isSparse => putSparseVector(vec, rowBuf, colBuf, valBuf)
        case vec: Vector if vec.isSorted => putSortedVector(vec, rowBuf, colBuf, valBuf)
      }
    }

    buf.position(valBuf.position())
  }

  def putVector(buf: ByteBuffer, vec: Vector, meta: Meta, head: DataHead): Unit = {
    assert(meta.shape.length == 2 && meta.shape(0) == 1)


    val oldOder = buf.order()
    buf.order(ByteOrder.LITTLE_ENDIAN)
    val dataHead = if (head == null) {
      getDataHeadFromVector(vec, meta)
    } else {
      head
    }
    dataHead.put2Buf(buf)

    vec match {
      case v: IntDummyVector =>
        v.getIndices.foreach(idx => buf.putLong(idx.toLong))
      case v: LongDummyVector =>
        v.getIndices.foreach(idx => buf.putLong(idx))
      case v if v.isDense => putDenseVector(buf, vec, dataHead)
      case v if v.isSparse || v.isSorted => putSparseVector(buf, vec, dataHead)
    }



    buf.order(oldOder)


  }

  def getDataHeadFromVector(vec: Vector, meta: Meta): DataHead = {
    val nnz: Int = vec.getSize.toInt

    val (sparseDim: Int, denseDim: Int, dtype: Int, length: Long) = vec match {
      case v: IntDoubleVector =>
        if (v.isDense) {
          (-1, 1, DataHead.DT_DOUBLE, v.size().toLong * 8)
        } else {
          (1, 0, DataHead.DT_DOUBLE, v.size().toLong * 16)
        }
      case v: IntFloatVector =>
        if (v.isDense) {
          (-1, 1, DataHead.DT_FLOAT, v.size().toLong * 4)
        } else {
          (1, 0, DataHead.DT_FLOAT, v.size().toLong * 12)
        }
      case v: IntLongVector =>
        if (v.isDense) {
          (-1, 1, DataHead.DT_LONG, v.size().toLong * 8)
        } else {
          (1, 0, DataHead.DT_LONG, v.size().toLong * 16)
        }
      case v: IntIntVector =>
        if (v.isDense) {
          (-1, 1, DataHead.DT_INT, v.size().toLong * 4)
        } else {
          (1, 0, DataHead.DT_INT, v.size().toLong * 12)
        }
      case v: LongDoubleVector =>
        if (v.isDense) {
          (-1, 1, DataHead.DT_DOUBLE, v.size().toLong * 8)
        } else {
          println("OK")
          (1, 0, DataHead.DT_DOUBLE, v.size().toLong * 16)
        }
      case v: LongFloatVector =>
        if (v.isDense) {
          (-1, 1, DataHead.DT_FLOAT, v.size().toLong * 4)
        } else {
          (1, 0, DataHead.DT_FLOAT, v.size().toLong * 12)
        }
      case v: LongLongVector =>
        if (v.isDense) {
          (-1, 1, DataHead.DT_LONG, v.size().toLong * 8)
        } else {
          (1, 0, DataHead.DT_LONG, v.size().toLong * 16)
        }
      case v: LongIntVector =>
        if (v.isDense) {
          (-1, 1, DataHead.DT_INT, v.size().toLong * 4)
        } else {
          (1, 0, DataHead.DT_INT, v.size().toLong * 12)
        }
      case v: IntDummyVector => (1, -1, DataHead.DT_FLOAT, v.size().toLong * 8)
      case v: LongDummyVector => (1, -1, DataHead.DT_FLOAT, v.size().toLong * 8)
    }

    new DataHead(sparseDim, denseDim, meta.shape, nnz, dtype, length.toInt)
  }

  private def putDenseVector(buf: ByteBuffer, vec: Vector, dataHead: DataHead): Unit = {
    vec match {
      case v: IntDoubleVector =>
        println("WO JIAN LE GUI?")
        v.getStorage.getValues.foreach(value => { println(value); buf.putDouble(value)})
      case v: IntFloatVector =>
        v.getStorage.getValues.foreach(value => buf.putFloat(value))
      case v: IntLongVector =>
        v.getStorage.getValues.foreach(value => buf.putLong(value))
      case v: IntIntVector =>
        v.getStorage.getValues.foreach(value => buf.putInt(value))
      case _ => throw new Exception("vector type is not support for dense!")
    }
  }

  private def putSparseVector(buf: ByteBuffer, vec: Vector, dataHead: DataHead): Unit = {

    val bytes = buf.array()
    var start = buf.position
    val colBuf = ByteBuffer.wrap(bytes, start, dataHead.nnz * 8)
    colBuf.order(ByteOrder.LITTLE_ENDIAN)
    start += dataHead.nnz * 8

    val valBuf = ByteBuffer.wrap(bytes, start, bytes.length - start)
    valBuf.order(ByteOrder.LITTLE_ENDIAN)

    vec match {
      case v if v.isSparse => putSparseVector(vec, null, colBuf, valBuf)
      case v if v.isSorted => putSortedVector(vec, null, colBuf, valBuf)
    }


    start += valBuf.position()
    buf.position(start)
  }
}
