package com.tencent.angel.common

import java.nio.{ByteBuffer, ByteOrder}

import com.tencent.angel.ml.servingmath2.MFactory
import com.tencent.angel.ml.servingmath2.matrix._
import com.tencent.angel.ml.servingmath2.vector._


object Serializer {

  def putMatrix(buf: ByteBuffer, mat: Matrix, meta: Meta): Unit = {
    // DataHead(dim: Int, rows: Long, cols: Long, nnz: Int, dtype: Int, storageFlag: Int, length: Int)
    buf.order(ByteOrder.LITTLE_ENDIAN)
    mat match {
      case m: BlasMatrix => putBlasMatrix(buf, m, meta)
      case m: RowBasedMatrix[_] if isDummy(mat) => putDummyRBMatrix(buf, m, meta)
      case m: RowBasedMatrix[_] if isDense(mat) => putDenseRBMatrix(buf, m, meta)
      case m: RowBasedMatrix[_] if isSparse(mat) => putSparseRBMatrix(buf, m, meta)
      case m: RowBasedMatrix[_] if isSorted(mat) => putSortedRBMatrix(buf, m, meta)
      case _ => throw new Exception("matrix type is not supported!")
    }
  }

  private def isDense(mat: Matrix): Boolean = {
    mat.getRow(0).isDense
  }

  private def isSparse(mat: Matrix): Boolean = {
    mat.getRow(0).isSparse
  }

  private def isSorted(mat: Matrix): Boolean = {
    mat.getRow(0).isSorted
  }

  private def isDummy(mat: Matrix): Boolean = {
    mat.getRow(0) match {
      case _: IntDummyVector => true
      case _: LongDummyVector => true
      case _ => false
    }
  }

  def getDataHeadFromRBMatrix(mat: RowBasedMatrix[_], meta: Meta): DataHead = {
    var nnz: Int = 0
    val rowIds = mat.getRows.map{ case vec: Vector =>
      nnz += vec.getSize
      vec.getRowId.toLong
    }


    var idx = -1
    val isDense = rowIds.forall{ ridx =>
      idx += 1
      ridx == idx
    }
    val sparseDim: Int = if (isDense) 0 else 1
    val denseDim: Int = if (isDense) 2 else 1

    val (cols: Long, dtype: Int) = mat.getRow(0) match {
      case v: DoubleVector => (v.dim, DataHead.DT_DOUBLE)
      case v: FloatVector => (v.dim, DataHead.DT_FLOAT)
      case v: LongVector => (v.dim, DataHead.DT_LONG)
      case v: IntVector => (v.dim, DataHead.DT_INT)
      case v: Vector => (v.dim, DataHead.DT_FLOAT)
    }

    val length = if (sparseDim == 0) {
      dtype match {
        case dt if dt == DataHead.DT_INT => nnz * 4
        case dt if dt == DataHead.DT_LONG => nnz * 8
        case dt if dt == DataHead.DT_FLOAT => nnz * 4
        case dt if dt == DataHead.DT_DOUBLE => nnz * 8
      }
    } else {
      dtype match {
        case dt if dt == DataHead.DT_INT => nnz * 20
        case dt if dt == DataHead.DT_LONG => nnz * 24
        case dt if dt == DataHead.DT_FLOAT => nnz * 20
        case dt if dt == DataHead.DT_DOUBLE => nnz * 24
      }
    }
    // DataHead(val dim: Int, val shape: Array[Long], val nnz: Int, val dtype: Int, val storageFlag: Int, length: Int)
    new DataHead(sparseDim, denseDim, meta.shape, nnz, dtype, length)
  }

  private def putBlasMatrix(buf: ByteBuffer, mat: BlasMatrix, meta: Meta): Unit = {
    mat match {
      case m: BlasDoubleMatrix =>
        val data = m.getData
        val head = new DataHead(0, 2, Array[Long](m.getNumRows, m.getNumCols), data.length,
          DataHead.DT_DOUBLE, data.length * 8)
        head.put2Buf(buf)
        data.foreach(value => buf.putDouble(value))
      case m: BlasFloatMatrix =>
        val data = m.getData
        val head = new DataHead(0, 2, Array[Long](m.getNumRows, m.getNumCols), data.length,
          DataHead.DT_FLOAT, data.length * 4)
        head.put2Buf(buf)
        data.foreach(value => buf.putFloat(value))
    }
  }

  private def putDenseRBMatrix(buf: ByteBuffer, mat: RowBasedMatrix[_], meta: Meta): Unit = {
    buf.order(ByteOrder.LITTLE_ENDIAN)
    val dataHead = getDataHeadFromRBMatrix(mat, meta)
    buf.put(dataHead.toBytes)

    (0 until mat.getNumRows).foreach { i =>
      mat.getCol(i) match {
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

  private def putSparseRBMatrix(buf: ByteBuffer, mat: RowBasedMatrix[_], meta: Meta): Unit = {
    val dataHead = getDataHeadFromRBMatrix(mat, meta)
    buf.put(dataHead.toBytes)

    val bytes = buf.array()
    var start = buf.position
    val rowIdxs = ByteBuffer.wrap(bytes, start, dataHead.nnz * 8)
    rowIdxs.order(ByteOrder.LITTLE_ENDIAN)
    start += dataHead.nnz * 8

    val colIdxs = ByteBuffer.wrap(bytes, start, dataHead.nnz * 8)
    colIdxs.order(ByteOrder.LITTLE_ENDIAN)
    start += dataHead.nnz * 8

    val valbuf = ByteBuffer.wrap(bytes, start, bytes.length - start)
    valbuf.order(ByteOrder.LITTLE_ENDIAN)

    (0 until mat.getNumRows).foreach { rowIdx =>
      rowIdxs.putLong(rowIdx.toLong)

      mat.getCol(rowIdx) match {
        case v: IntDoubleVector =>
          val iter = v.getStorage.entryIterator()
          while (iter.hasNext) {
            val entry = iter.next()
            rowIdxs.putLong(rowIdx.toLong)
            colIdxs.putLong(entry.getIntKey.toLong)
            valbuf.putDouble(entry.getDoubleValue)
          }
        case v: IntFloatVector =>
          val iter = v.getStorage.entryIterator()
          while (iter.hasNext) {
            val entry = iter.next()
            rowIdxs.putLong(rowIdx.toLong)
            colIdxs.putLong(entry.getIntKey.toLong)
            valbuf.putFloat(entry.getFloatValue)
          }
        case v: IntLongVector =>
          val iter = v.getStorage.entryIterator()
          while (iter.hasNext) {
            val entry = iter.next()
            rowIdxs.putLong(rowIdx.toLong)
            colIdxs.putLong(entry.getIntKey.toLong)
            valbuf.putLong(entry.getLongValue)
          }
        case v: IntIntVector =>
          val iter = v.getStorage.entryIterator()
          while (iter.hasNext) {
            val entry = iter.next()
            rowIdxs.putLong(rowIdx.toLong)
            colIdxs.putLong(entry.getIntKey.toLong)
            valbuf.putInt(entry.getIntValue)
          }
        case v: LongDoubleVector =>
          val iter = v.getStorage.entryIterator()
          while (iter.hasNext) {
            val entry = iter.next()
            rowIdxs.putLong(rowIdx.toLong)
            colIdxs.putLong(entry.getLongKey)
            valbuf.putDouble(entry.getDoubleValue)
          }
        case v: LongFloatVector =>
          val iter = v.getStorage.entryIterator()
          while (iter.hasNext) {
            val entry = iter.next()
            rowIdxs.putLong(rowIdx.toLong)
            colIdxs.putLong(entry.getLongKey)
            valbuf.putFloat(entry.getFloatValue)
          }
        case v: LongLongVector =>
          val iter = v.getStorage.entryIterator()
          while (iter.hasNext) {
            val entry = iter.next()
            rowIdxs.putLong(rowIdx.toLong)
            colIdxs.putLong(entry.getLongKey)
            valbuf.putLong(entry.getLongValue)
          }
        case v: LongIntVector =>
          val iter = v.getStorage.entryIterator()
          while (iter.hasNext) {
            val entry = iter.next()
            rowIdxs.putLong(rowIdx.toLong)
            colIdxs.putLong(entry.getLongKey)
            valbuf.putInt(entry.getIntValue)
          }
      }
    }

    start += valbuf.position()
    buf.position(start)
  }

  private def putSortedRBMatrix(buf: ByteBuffer, mat: RowBasedMatrix[_], meta: Meta): Unit = {
    val dataHead = getDataHeadFromRBMatrix(mat, meta)
    buf.put(dataHead.toBytes)

    val bytes = buf.array()
    var start = buf.position
    val rowIdxs = ByteBuffer.wrap(bytes, start, dataHead.nnz * 8)
    rowIdxs.order(ByteOrder.LITTLE_ENDIAN)
    start += dataHead.nnz * 8

    val colIdxs = ByteBuffer.wrap(bytes, start, dataHead.nnz * 8)
    colIdxs.order(ByteOrder.LITTLE_ENDIAN)
    start += dataHead.nnz * 8

    val valbuf = ByteBuffer.wrap(bytes, start, bytes.length - start)
    valbuf.order(ByteOrder.LITTLE_ENDIAN)

    (0 until mat.getNumRows).foreach { rowIdx =>
      mat.getCol(rowIdx) match {
        case v: IntDoubleVector =>
          val indices = v.getStorage.getIndices
          val values = v.getStorage.getValues
          indices.foreach { idx =>
            rowIdxs.putLong(rowIdx.toLong)
            rowIdxs.putLong(idx.toLong)
          }

          values.foreach(value => valbuf.putDouble(value))
        case v: IntFloatVector =>
          val indices = v.getStorage.getIndices
          val values = v.getStorage.getValues
          indices.foreach { idx =>
            rowIdxs.putLong(rowIdx.toLong)
            rowIdxs.putLong(idx.toLong)
          }

          values.foreach(value => valbuf.putFloat(value))
        case v: IntLongVector =>
          val indices = v.getStorage.getIndices
          val values = v.getStorage.getValues
          indices.foreach { idx =>
            rowIdxs.putLong(rowIdx.toLong)
            rowIdxs.putLong(idx.toLong)
          }

          values.foreach(value => valbuf.putLong(value))
        case v: IntIntVector =>
          val indices = v.getStorage.getIndices
          val values = v.getStorage.getValues
          indices.foreach { idx =>
            rowIdxs.putLong(rowIdx.toLong)
            rowIdxs.putLong(idx.toLong)
          }

          values.foreach(value => valbuf.putInt(value))
        case v: LongDoubleVector =>
          val indices = v.getStorage.getIndices
          val values = v.getStorage.getValues
          indices.foreach { idx =>
            rowIdxs.putLong(rowIdx.toLong)
            rowIdxs.putLong(idx)
          }

          values.foreach(value => valbuf.putDouble(value))
        case v: LongFloatVector =>
          val indices = v.getStorage.getIndices
          val values = v.getStorage.getValues
          indices.foreach { idx =>
            rowIdxs.putLong(rowIdx.toLong)
            rowIdxs.putLong(idx)
          }

          values.foreach(value => valbuf.putFloat(value))
        case v: LongLongVector =>
          val indices = v.getStorage.getIndices
          val values = v.getStorage.getValues
          indices.foreach { idx =>
            rowIdxs.putLong(rowIdx.toLong)
            rowIdxs.putLong(idx)
          }

          values.foreach(value => valbuf.putLong(value))
        case v: LongIntVector =>
          val indices = v.getStorage.getIndices
          val values = v.getStorage.getValues
          indices.foreach { idx =>
            rowIdxs.putLong(rowIdx.toLong)
            rowIdxs.putLong(idx)
          }

          values.foreach(value => valbuf.putInt(value))
      }
    }

    start += valbuf.position()
    buf.position(start)
  }

  private def putDummyRBMatrix(buf: ByteBuffer, mat: RowBasedMatrix[_], meta: Meta): Unit = {
    val dataHead = getDataHeadFromRBMatrix(mat, meta)
    buf.put(dataHead.toBytes)

    val bytes = buf.array()
    var start = buf.position
    val rowIdxs = ByteBuffer.wrap(bytes, start, dataHead.nnz * 8)
    rowIdxs.order(ByteOrder.LITTLE_ENDIAN)
    start += dataHead.nnz * 8

    val colIdxs = ByteBuffer.wrap(bytes, start, dataHead.nnz * 8)
    colIdxs.order(ByteOrder.LITTLE_ENDIAN)
    start += dataHead.nnz * 8

    buf.position(start)

    (0 until mat.getNumRows).foreach { rowIdx =>
      mat.getCol(rowIdx) match {
        case v: IntDummyVector =>
          val indices = v.getIndices
          indices.foreach { idx =>
            rowIdxs.putLong(rowIdx.toLong)
            rowIdxs.putLong(idx.toLong)
          }
        case v: LongDummyVector =>
          val indices = v.getIndices
          indices.foreach { idx =>
            rowIdxs.putLong(rowIdx.toLong)
            rowIdxs.putLong(idx)
          }
      }
    }
  }

  def putVector(buf: ByteBuffer, vec: Vector): Unit = {
    buf.order(ByteOrder.LITTLE_ENDIAN)

    vec match {
      case v: IntDummyVector =>
        val dataHead = getDataHeadFromVector(v)
        buf.put(dataHead.toBytes)
        v.getIndices.foreach(idx => buf.putLong(idx.toLong))
      case v: LongDummyVector =>
        val dataHead = getDataHeadFromVector(v)
        buf.put(dataHead.toBytes)
        v.getIndices.foreach(idx => buf.putLong(idx))
      case v if v.isDense => putDenseVector(buf, vec)
      case v if v.isSparse => putSparseVector(buf, vec)
      case v if v.isSorted => putSortedVector(buf, vec)
    }
  }

  def getDataHeadFromVector(vec: Vector): DataHead = {
    // DataHead(val dim: Int, val shape: Array[Long], val nnz: Int, val dtype: Int, val storageFlag: Int, length: Int)

    val (nnz: Int, dtype: Int, length: Int) = vec match {
      case v: IntDoubleVector =>
        if (v.isDense) {
          (v.size(), DataHead.DT_DOUBLE, v.size() * 8)
        } else {
          (v.size(), DataHead.DT_DOUBLE, v.size() * 16)
        }
      case v: IntFloatVector =>
        if (v.isDense) {
          (v.size(), DataHead.DT_FLOAT, v.size() * 4)
        } else {
          (v.size(), DataHead.DT_FLOAT, v.size() * 12)
        }
      case v: IntLongVector =>
        if (v.isDense) {
          (v.size(), DataHead.DT_LONG, v.size() * 8)
        } else {
          (v.size(), DataHead.DT_LONG, v.size() * 16)
        }
      case v: IntIntVector =>
        if (v.isDense) {
          (v.size(), DataHead.DT_INT, v.size() * 4)
        } else {
          (v.size(), DataHead.DT_INT, v.size() * 12)
        }
      case v: LongDoubleVector =>
        if (v.isDense) {
          (v.size(), DataHead.DT_DOUBLE, v.size() * 8)
        } else {
          (v.size(), DataHead.DT_DOUBLE, v.size() * 16)
        }
      case v: LongFloatVector =>
        if (v.isDense) {
          (v.size(), DataHead.DT_FLOAT, v.size() * 4)
        } else {
          (v.size(), DataHead.DT_FLOAT, v.size() * 12)
        }
      case v: LongLongVector =>
        if (v.isDense) {
          (v.size(), DataHead.DT_LONG, v.size() * 8)
        } else {
          (v.size(), DataHead.DT_LONG, v.size() * 16)
        }
      case v: LongIntVector =>
        if (v.isDense) {
          (v.size(), DataHead.DT_INT, v.size() * 4)
        } else {
          (v.size(), DataHead.DT_INT, v.size() * 12)
        }
      case v: IntDummyVector => (v.size(), DataHead.DT_FLOAT, v.size() * 8)
      case v: LongDummyVector => (v.size(), DataHead.DT_FLOAT, v.size() * 8)
      case _ =>
    }

    val sparseDim = if (vec.getType.isDense) 0 else 1
    val denseDim = if (vec.getType.isDense) 1 else 0
    new DataHead(sparseDim, denseDim, Array[Long](vec.dim()), nnz, dtype, length)
  }

  private def putDenseVector(buf: ByteBuffer, vec: Vector): Unit = {
    val dataHead = getDataHeadFromVector(vec)
    dataHead.put2Buf(buf)

    vec match {
      case v: IntDoubleVector =>
        v.getStorage.getValues.foreach(value => buf.putDouble(value))
      case v: IntFloatVector =>
        v.getStorage.getValues.foreach(value => buf.putFloat(value))
      case v: IntLongVector =>
        v.getStorage.getValues.foreach(value => buf.putLong(value))
      case v: IntIntVector =>
        v.getStorage.getValues.foreach(value => buf.putInt(value))
      case _ => throw new Exception("vector type is not support for dense!")
    }
  }

  private def putSparseVector(buf: ByteBuffer, vec: Vector): Unit = {
    val dataHead = getDataHeadFromVector(vec)
    dataHead.put2Buf(buf)

    val bytes = buf.array()
    var start = buf.position
    val idxs = ByteBuffer.wrap(bytes, start, dataHead.nnz * 8)
    idxs.order(ByteOrder.LITTLE_ENDIAN)
    start += dataHead.nnz * 8

    val valbuf = ByteBuffer.wrap(bytes, start, bytes.length - start)
    valbuf.order(ByteOrder.LITTLE_ENDIAN)

    vec match {
      case v: IntDoubleVector =>
        val iter = v.getStorage.entryIterator()
        while (iter.hasNext) {
          val entry = iter.next()
          idxs.putLong(entry.getIntKey.toLong)
          idxs.putDouble(entry.getDoubleValue)
        }
      case v: IntFloatVector =>
        val iter = v.getStorage.entryIterator()
        while (iter.hasNext) {
          val entry = iter.next()
          idxs.putLong(entry.getIntKey.toLong)
          idxs.putDouble(entry.getFloatValue)
        }
      case v: IntLongVector =>
        val iter = v.getStorage.entryIterator()
        while (iter.hasNext) {
          val entry = iter.next()
          idxs.putLong(entry.getIntKey.toLong)
          idxs.putDouble(entry.getLongValue)
        }
      case v: IntIntVector =>
        val iter = v.getStorage.entryIterator()
        while (iter.hasNext) {
          val entry = iter.next()
          idxs.putLong(entry.getIntKey.toLong)
          idxs.putDouble(entry.getIntValue)
        }
      case v: LongDoubleVector =>
        val iter = v.getStorage.entryIterator()
        while (iter.hasNext) {
          val entry = iter.next()
          idxs.putLong(entry.getLongKey)
          idxs.putDouble(entry.getDoubleValue)
        }
      case v: LongFloatVector =>
        val iter = v.getStorage.entryIterator()
        while (iter.hasNext) {
          val entry = iter.next()
          idxs.putLong(entry.getLongKey)
          idxs.putDouble(entry.getFloatValue)
        }
      case v: LongLongVector =>
        val iter = v.getStorage.entryIterator()
        while (iter.hasNext) {
          val entry = iter.next()
          idxs.putLong(entry.getLongKey)
          idxs.putDouble(entry.getLongValue)
        }
      case v: LongIntVector =>
        val iter = v.getStorage.entryIterator()
        while (iter.hasNext) {
          val entry = iter.next()
          idxs.putLong(entry.getLongKey)
          idxs.putDouble(entry.getIntValue)
        }
      case _ => throw new Exception("vector type is not support for dense!")
    }

    start += valbuf.position()
    buf.position(start)
  }

  private def putSortedVector(buf: ByteBuffer, vec: Vector): Unit = {
    val dataHead = getDataHeadFromVector(vec)
    dataHead.put2Buf(buf)

    val bytes = buf.array()
    var start = buf.position
    val idxs = ByteBuffer.wrap(bytes, start, dataHead.nnz * 8)
    idxs.order(ByteOrder.LITTLE_ENDIAN)
    start += dataHead.nnz * 8

    val valbuf = ByteBuffer.wrap(bytes, start, bytes.length - start)
    valbuf.order(ByteOrder.LITTLE_ENDIAN)

    vec match {
      case v: IntDoubleVector =>
        val indices = v.getStorage.getIndices
        val values = v.getStorage.getValues

        indices.foreach(i => buf.putLong(i.toLong))
        values.foreach(v => buf.putDouble(v))
      case v: IntFloatVector =>
        val indices = v.getStorage.getIndices
        val values = v.getStorage.getValues

        indices.foreach(i => buf.putLong(i.toLong))
        values.foreach(v => buf.putDouble(v))
      case v: IntLongVector =>
        val indices = v.getStorage.getIndices
        val values = v.getStorage.getValues

        indices.foreach(i => buf.putLong(i.toLong))
        values.foreach(v => buf.putLong(v))
      case v: IntIntVector =>
        val indices = v.getStorage.getIndices
        val values = v.getStorage.getValues

        indices.foreach(i => buf.putLong(i.toLong))
        values.foreach(v => buf.putInt(v))
      case v: LongDoubleVector =>
        val indices = v.getStorage.getIndices
        val values = v.getStorage.getValues

        indices.foreach(i => buf.putLong(i))
        values.foreach(v => buf.putDouble(v))
      case v: LongFloatVector =>
        val indices = v.getStorage.getIndices
        val values = v.getStorage.getValues

        indices.foreach(i => buf.putLong(i))
        values.foreach(v => buf.putFloat(v))
      case v: LongLongVector =>
        val indices = v.getStorage.getIndices
        val values = v.getStorage.getValues

        indices.foreach(i => buf.putLong(i))
        values.foreach(v => buf.putLong(v))
      case v: LongIntVector =>
        val indices = v.getStorage.getIndices
        val values = v.getStorage.getValues

        indices.foreach(i => buf.putLong(i))
        values.foreach(v => buf.putInt(v))
      case _ => throw new Exception("vector type is not support for dense!")
    }

    start += valbuf.position()
    buf.position(start)
  }
}
