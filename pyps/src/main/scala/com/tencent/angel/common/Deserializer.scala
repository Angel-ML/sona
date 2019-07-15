package com.tencent.angel.common

import java.nio.{ByteBuffer, ByteOrder}
import java.util

import com.tencent.angel.ml.servingmath2.{MFactory, VFactory}
import com.tencent.angel.ml.servingmath2.matrix._
import com.tencent.angel.ml.servingmath2.vector._
import it.unimi.dsi.fastutil.ints.IntOpenHashSet
import it.unimi.dsi.fastutil.longs.LongOpenHashSet

import scala.collection.mutable.ListBuffer

object Deserializer {
  private def isIntKey(meta: Meta): Boolean = {
    meta.getMatrixContext.getRowType.isIntKey
  }

  private def isLongKey(meta: Meta): Boolean = {
    meta.getMatrixContext.getRowType.isLongKey
  }

  private def checkValueType(dataHead: DataHead, meta: Meta): Boolean = {
    dataHead.dtype match {
      case dt if dt == DataHead.DT_DOUBLE =>
        meta.getMatrixContext.getRowType.isDouble
      case dt if dt == DataHead.DT_FLOAT =>
        meta.getMatrixContext.getRowType.isFloat
      case dt if dt == DataHead.DT_LONG =>
        meta.getMatrixContext.getRowType.isLong
      case dt if dt == DataHead.DT_INT =>
        meta.getMatrixContext.getRowType.isInt
    }
  }

  def matrixFromBuffer(buf: ByteBuffer, meta: Meta): Matrix = {
    val dataHead = DataHead.fromBuffer(buf)
    if (dataHead.shape.length >= 2) {
      assert(util.Arrays.equals(meta.getShape, dataHead.shape))
      assert(checkValueType(dataHead, meta))

      if (dataHead.sparseDim == 1 && dataHead.denseDim >= 1) { // sparse (the first dim is sparse)
        sparseMatrixFromBuffer1(buf, dataHead, meta)
      } else if (dataHead.sparseDim == 2 && dataHead.denseDim == 0) { // sparse (both dim is sparse)
        sparseMatrixFromBuffer2(buf, dataHead, meta)
      } else if (dataHead.sparseDim == -1 && dataHead.denseDim >= 2) { // dense
        denseMatrixFromBuffer(buf, dataHead, meta)
      } else {
        throw new Exception("sparseDim or denseDim is invalidate!")
      }
    } else {
      null
    }
  }

  private def vectorArray2Matrix(vectors: Array[Vector]): Matrix = {
    vectors.head match {
      case _: IntDoubleVector => MFactory.rbIntDoubleMatrix(vectors.map(_.asInstanceOf[IntDoubleVector]))
      case _: IntFloatVector => MFactory.rbIntFloatMatrix(vectors.map(_.asInstanceOf[IntFloatVector]))
      case _: IntLongVector => MFactory.rbIntLongMatrix(vectors.map(_.asInstanceOf[IntLongVector]))
      case _: IntIntVector => MFactory.rbIntIntMatrix(vectors.map(_.asInstanceOf[IntIntVector]))
      case _: LongDoubleVector => MFactory.rbLongDoubleMatrix(vectors.map(_.asInstanceOf[LongDoubleVector]))
      case _: LongFloatVector => MFactory.rbLongFloatMatrix(vectors.map(_.asInstanceOf[LongFloatVector]))
      case _: LongLongVector => MFactory.rbLongLongMatrix(vectors.map(_.asInstanceOf[LongLongVector]))
      case _: LongIntVector => MFactory.rbLongIntMatrix(vectors.map(_.asInstanceOf[LongIntVector]))
    }
  }

  private def sparseMatrixFromBuffer1(buf: ByteBuffer, dataHead: DataHead, meta: Meta): Matrix = {
    assert(dataHead.sparseDim == 1 && dataHead.denseDim >= 1)

    val matrixId = meta.getMatrixContext.getMatrixId
    val rows = dataHead.nnz
    val cols = {
      var count = 1
      (1 until dataHead.shape.length).foreach { i =>
        count *= dataHead.shape(i)
      }

      count
    }

    val bytes = buf.array()
    var start: Int = buf.position()
    val rowIdxs = ByteBuffer.wrap(bytes, start, dataHead.nnz * 8)
    rowIdxs.order(ByteOrder.LITTLE_ENDIAN)
    start += dataHead.nnz * 8

    val valBuf = ByteBuffer.wrap(bytes, start, bytes.length - start)
    valBuf.order(ByteOrder.LITTLE_ENDIAN)

    val vectors: Array[Vector] = (0 until rows).toArray.map { _ =>
      val rowId = rowIdxs.getLong.toInt
      if (dataHead.dtype == DataHead.DT_DOUBLE) {
        val row = new Array[Double](cols)
        var i = 0
        while (i < cols) {
          row(i) = valBuf.getDouble()
          i += 1
        }

        VFactory.denseDoubleVector(matrixId, rowId, 0, row)
      } else if (dataHead.dtype == DataHead.DT_FLOAT) {
        val row = new Array[Float](cols)
        var i = 0
        while (i < cols) {
          row(i) = buf.getFloat()
          i += 1
        }

        VFactory.denseFloatVector(matrixId, rowId, 0, row)
      } else if (dataHead.dtype == DataHead.DT_LONG) {
        val row = new Array[Long](cols)
        var i = 0
        while (i < cols) {
          row(i) = buf.getLong()
          i += 1
        }

        VFactory.denseLongVector(matrixId, rowId, 0, row)
      } else {
        val row = new Array[Int](cols)
        var i = 0
        while (i < cols) {
          row(i) = buf.getInt()
          i += 1
        }

        VFactory.denseIntVector(matrixId, rowId, 0, row)
      }
    }

    start += valBuf.position()
    buf.position(start)
    vectorArray2Matrix(vectors)
  }

  private def sparseMatrixFromBuffer2(buf: ByteBuffer, dataHead: DataHead, meta: Meta): Matrix = {
    assert(dataHead.shape.length == 2)
    assert(dataHead.sparseDim == 2 && dataHead.denseDim == 0) // matrix (both dim is sparse)

    val matrixId = meta.getMatrixContext.getMatrixId

    val bytes = buf.array()
    var start: Int = buf.position()
    val rowIdxs = ByteBuffer.wrap(bytes, start, dataHead.nnz * 8)
    rowIdxs.order(ByteOrder.LITTLE_ENDIAN)
    start += dataHead.nnz * 8

    val colIdx = ByteBuffer.wrap(bytes, start, dataHead.nnz * 8)
    colIdx.order(ByteOrder.LITTLE_ENDIAN)
    start += dataHead.nnz * 8

    val valBuf = ByteBuffer.wrap(bytes, start, bytes.length - start)
    valBuf.order(ByteOrder.LITTLE_ENDIAN)

    val colDim = dataHead.shape(1)
    var lastRowId: Long = -1L
    val csrRow = new ListBuffer[Int]()
    (0 until dataHead.nnz).foreach { i =>
      val rowId = buf.getLong()
      if (lastRowId != rowId) {
        lastRowId = rowId
        csrRow.append(i)
      }
    }
    csrRow.append(dataHead.nnz)
    val rowIdx = csrRow.toArray

    val vectors: Array[Vector] = (0 until rowIdx.length - 1).toArray.map { i =>
      val len = rowIdx(i + 1) - rowIdx(i)
      val rowId = rowIdxs.getLong(rowIdx(i)).toInt

      if (dataHead.dtype == DataHead.DT_DOUBLE && isIntKey(meta)) {
        val indices = new Array[Int](len)
        val values = new Array[Double](len)
        var j = 0
        while (j < len) {
          indices(j) = colIdx.getLong.toInt
          values(j) = valBuf.getDouble
          j += 1
        }
        VFactory.sortedDoubleVector(matrixId, rowId, 0, colDim.toInt, indices, values)
      } else if (dataHead.dtype == DataHead.DT_DOUBLE && isLongKey(meta)) {
        val indices = new Array[Long](len)
        val values = new Array[Double](len)
        var j = 0
        while (j < len) {
          indices(j) = colIdx.getLong
          values(j) = valBuf.getDouble
          j += 1
        }
        VFactory.sortedLongKeyDoubleVector(matrixId, rowId, 0, colDim, indices, values)
      } else if (dataHead.dtype == DataHead.DT_FLOAT && isIntKey(meta)) {
        val indices = new Array[Int](len)
        val values = new Array[Float](len)
        var j = 0
        while (j < len) {
          indices(j) = colIdx.getLong.toInt
          values(j) = valBuf.getFloat
          j += 1
        }
        VFactory.sortedFloatVector(matrixId, rowId, 0, colDim.toInt, indices, values)
      } else if (dataHead.dtype == DataHead.DT_FLOAT && isLongKey(meta)) {
        val indices = new Array[Long](len)
        val values = new Array[Float](len)
        var j = 0
        while (j < len) {
          indices(j) = colIdx.getLong
          values(j) = valBuf.getFloat
          j += 1
        }
        VFactory.sortedLongKeyFloatVector(matrixId, rowId, 0, colDim, indices, values)
      } else if (dataHead.dtype == DataHead.DT_LONG && isIntKey(meta)) {
        val indices = new Array[Int](len)
        val values = new Array[Long](len)
        var j = 0
        while (j < len) {
          indices(j) = colIdx.getLong.toInt
          values(j) = valBuf.getLong
          j += 1
        }
        VFactory.sortedLongVector(matrixId, rowId, 0, colDim.toInt, indices, values)
      } else if (dataHead.dtype == DataHead.DT_LONG && isLongKey(meta)) {
        val indices = new Array[Long](len)
        val values = new Array[Long](len)
        var j = 0
        while (j < len) {
          indices(j) = colIdx.getLong
          values(j) = valBuf.getLong
          j += 1
        }
        VFactory.sortedLongKeyLongVector(matrixId, rowId, 0, colDim, indices, values)
      } else if (dataHead.dtype == DataHead.DT_INT && isIntKey(meta)) {
        val indices = new Array[Int](len)
        val values = new Array[Int](len)
        var j = 0
        while (j < len) {
          indices(j) = colIdx.getLong.toInt
          values(j) = valBuf.getInt
          j += 1
        }
        VFactory.sortedIntVector(matrixId, rowId, 0, colDim.toInt, indices, values)
      } else if (dataHead.dtype == DataHead.DT_INT && isLongKey(meta)) {
        val indices = new Array[Long](len)
        val values = new Array[Int](len)
        var j = 0
        while (j < len) {
          indices(j) = colIdx.getLong
          values(j) = valBuf.getInt
          j += 1
        }
        VFactory.sortedLongKeyIntVector(matrixId, rowId, 0, colDim, indices, values)
      } else {
        throw new Exception("Unknown data type!")
      }
    }

    start += valBuf.position()
    buf.position(start)
    vectorArray2Matrix(vectors)
  }

  private def denseMatrixFromBuffer(buf: ByteBuffer, dataHead: DataHead, meta: Meta): Matrix = {
    assert(util.Arrays.equals(dataHead.shape, meta.shape))
    assert(dataHead.sparseDim == -1 && dataHead.denseDim >= 2) // matrix, blas

    val matrixId = meta.getMatrixContext.getMatrixId

    val rows = dataHead.shape(0).toInt
    val cols = dataHead.nnz / dataHead.shape(0).toInt

    val oldOrder = buf.order()
    buf.order(ByteOrder.LITTLE_ENDIAN)
    val mat = if (dataHead.dtype == DataHead.DT_DOUBLE) {
      val values = new Array[Double](dataHead.nnz)
      var i = 0
      while (i < dataHead.nnz) {
        values(i) = buf.getDouble
        i += 1
      }
      MFactory.denseDoubleMatrix(matrixId, 0, rows, cols, values)
    } else if (dataHead.dtype == DataHead.DT_FLOAT) {
      val values = new Array[Float](dataHead.nnz)
      var i = 0
      while (i < dataHead.nnz) {
        values(i) = buf.getFloat
        i += 1
      }
      MFactory.denseFloatMatrix(matrixId, 0, rows, cols, values)
    } else {
      throw new Exception("Unknown data type!")
    }

    buf.order(oldOrder)
    mat
  }

  def vectorFromBuffer(buf: ByteBuffer, meta: Meta): Vector = {
    buf.order(ByteOrder.LITTLE_ENDIAN)

    val dataHead = DataHead.fromBuffer(buf)
    if (dataHead.shape.length > 1) {
      if (dataHead.sparseDim == -1 && dataHead.denseDim == 1) {
        denseVectorFromBuffer(buf, dataHead, meta)
      } else if (dataHead.sparseDim == 1 && dataHead.denseDim == 0) {
        sparseVectorFromBuffer(buf, dataHead, meta)
      } else if (dataHead.sparseDim == 1 && dataHead.denseDim == -1) {
        dummyVectorFromBuffer(buf, dataHead, meta)
      } else {
        throw new Exception("sparseDim or denseDim is invalidate!")
      }
    } else {
      null
    }
  }

  private def dummyVectorFromBuffer(buf: ByteBuffer, dataHead: DataHead, meta: Meta): Vector = {}

  private def sparseVectorFromBuffer(buf: ByteBuffer, dataHead: DataHead, meta: Meta): Vector = {}

  private def denseVectorFromBuffer(buf: ByteBuffer, dataHead: DataHead, meta: Meta): Vector = {}

  def indicesFromBuffer(buf: ByteBuffer, dataHead: DataHead, meta: Meta): Vector = {
    assert(meta.getMatrixContext.getRowType.isSparse)

    val bytes = buf.array()
    var start = buf.position()
    val inxBuf = if (dataHead.sparseDim == 1) {
      ByteBuffer.wrap(bytes, start, dataHead.nnz * 8)
    } else if (dataHead.sparseDim == 2) {
      start += dataHead.nnz * 8
      ByteBuffer.wrap(bytes, start, dataHead.nnz * 8)
    } else {
      throw new Exception("dim > 2 not supported!")
    }

    inxBuf.order(ByteOrder.LITTLE_ENDIAN)

    if (isIntKey(meta)) {
      val hashSet = new IntOpenHashSet(dataHead.nnz)
      var i = 0
      while (i < dataHead.nnz) {
        hashSet.add(inxBuf.getLong.toInt)
        i += 1
      }

      val data = hashSet.toIntArray
      util.Arrays.sort(data)
      VFactory.denseIntVector(data)
    } else {
      val hashSet = new LongOpenHashSet(dataHead.nnz)
      var i = 0
      while (i < dataHead.nnz) {
        hashSet.add(inxBuf.getLong)
        i += 1
      }

      val data = hashSet.toLongArray
      util.Arrays.sort(data)
      VFactory.denseLongVector(data)
    }
  }

}
