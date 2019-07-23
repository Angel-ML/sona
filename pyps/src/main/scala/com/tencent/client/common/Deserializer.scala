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
    println(dataHead.denseDim, dataHead.dtype, dataHead.length, dataHead.shape)
    if (dataHead.shape.length >= 2) {
      assert(util.Arrays.equals(meta.shape, dataHead.shape))
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
      println("here we go//// ")
      null
    }
  }

  private def getDenseVector(matrixId: Int, rowId:Int, dim: Int, dataHead: DataHead, valBuf: ByteBuffer): Vector = {
    if (dataHead.dtype == DataHead.DT_DOUBLE) {
      val row = new Array[Double](dim)
      var i = 0
      while (i < dim) {
        row(i) = valBuf.getDouble()
        i += 1
      }

      VFactory.denseDoubleVector(matrixId, rowId, 0, row)
    } else if (dataHead.dtype == DataHead.DT_FLOAT) {
      val row = new Array[Float](dim)
      var i = 0
      while (i < dim) {
        row(i) = valBuf.getFloat()
        i += 1
      }

      VFactory.denseFloatVector(matrixId, rowId, 0, row)
    } else if (dataHead.dtype == DataHead.DT_LONG) {
      val row = new Array[Long](dim)
      var i = 0
      while (i < dim) {
        row(i) = valBuf.getLong()
        i += 1
      }

      VFactory.denseLongVector(matrixId, rowId, 0, row)
    } else {
      val row = new Array[Int](dim)
      var i = 0
      while (i < dim) {
        row(i) = valBuf.getInt()
        i += 1
      }

      VFactory.denseIntVector(matrixId, rowId, 0, row)
    }
  }

  private def getSparseVector(matrixId: Int, rowId:Int, len: Int, dim: Long, dataHead: DataHead, meta: Meta,
                  colBuf: ByteBuffer, valBuf: ByteBuffer): Vector = {
    if (dataHead.dtype == DataHead.DT_DOUBLE && isIntKey(meta)) {
      val indices = new Array[Int](len)
      val values = new Array[Double](len)
      var j = 0
      while (j < len) {
        indices(j) = colBuf.getLong.toInt
        values(j) = valBuf.getDouble
        j += 1
      }
      VFactory.sortedDoubleVector(matrixId, rowId, 0, dim.toInt, indices, values)
    } else if (dataHead.dtype == DataHead.DT_DOUBLE && isLongKey(meta)) {
      val indices = new Array[Long](len)
      val values = new Array[Double](len)
      var j = 0
      while (j < len) {
        indices(j) = colBuf.getLong
        values(j) = valBuf.getDouble
        j += 1
      }
      VFactory.sortedLongKeyDoubleVector(matrixId, rowId, 0, dim, indices, values)
    } else if (dataHead.dtype == DataHead.DT_FLOAT && isIntKey(meta)) {
      val indices = new Array[Int](len)
      val values = new Array[Float](len)
      var j = 0
      while (j < len) {
        indices(j) = colBuf.getLong.toInt
        values(j) = valBuf.getFloat
        j += 1
      }
      VFactory.sortedFloatVector(matrixId, rowId, 0, dim.toInt, indices, values)
    } else if (dataHead.dtype == DataHead.DT_FLOAT && isLongKey(meta)) {
      val indices = new Array[Long](len)
      val values = new Array[Float](len)
      var j = 0
      while (j < len) {
        indices(j) = colBuf.getLong
        values(j) = valBuf.getFloat
        j += 1
      }
      VFactory.sortedLongKeyFloatVector(matrixId, rowId, 0, dim, indices, values)
    } else if (dataHead.dtype == DataHead.DT_LONG && isIntKey(meta)) {
      val indices = new Array[Int](len)
      val values = new Array[Long](len)
      var j = 0
      while (j < len) {
        indices(j) = colBuf.getLong.toInt
        values(j) = valBuf.getLong
        j += 1
      }
      VFactory.sortedLongVector(matrixId, rowId, 0, dim.toInt, indices, values)
    } else if (dataHead.dtype == DataHead.DT_LONG && isLongKey(meta)) {
      val indices = new Array[Long](len)
      val values = new Array[Long](len)
      var j = 0
      while (j < len) {
        indices(j) = colBuf.getLong
        values(j) = valBuf.getLong
        j += 1
      }
      VFactory.sortedLongKeyLongVector(matrixId, rowId, 0, dim, indices, values)
    } else if (dataHead.dtype == DataHead.DT_INT && isIntKey(meta)) {
      val indices = new Array[Int](len)
      val values = new Array[Int](len)
      var j = 0
      while (j < len) {
        indices(j) = colBuf.getLong.toInt
        values(j) = valBuf.getInt
        j += 1
      }
      VFactory.sortedIntVector(matrixId, 0, rowId, dim.toInt, indices, values)
    } else if (dataHead.dtype == DataHead.DT_INT && isLongKey(meta)) {
      val indices = new Array[Long](len)
      val values = new Array[Int](len)
      var j = 0
      while (j < len) {
        indices(j) = colBuf.getLong
        values(j) = valBuf.getInt
        j += 1
      }
      VFactory.sortedLongKeyIntVector(matrixId, rowId, 0, dim, indices, values)
    } else {
      throw new Exception("Unknown data type!")
    }
  }

  private def sparseMatrixFromBuffer1(buf: ByteBuffer, dataHead: DataHead, meta: Meta): Matrix = {
    assert(dataHead.sparseDim == 1 && dataHead.denseDim >= 1)

    val matrixId = meta.getMatrixContext.getMatrixId
    val dim = {
      var count = 1
      (1 until dataHead.shape.length).foreach { i =>
        count *= dataHead.shape(i).toInt
      }

      count
    }

    val bytes = buf.array()
    var start: Int = buf.position()
    val rowBuf = ByteBuffer.wrap(bytes, start, dataHead.nnz * 8)
    rowBuf.order(ByteOrder.LITTLE_ENDIAN)
    start += dataHead.nnz * 8

    val valBuf = ByteBuffer.wrap(bytes, start, bytes.length - start)
    valBuf.order(ByteOrder.LITTLE_ENDIAN)

    val vectors: Array[Vector] = (0 until dataHead.nnz).toArray.map { _ =>
      getDenseVector(matrixId, rowBuf.getLong.toInt, dim, dataHead, valBuf)
    }

    buf.position(valBuf.position())
    Utils.vectorArray2Matrix(vectors)
  }

  private def sparseMatrixFromBuffer2(buf: ByteBuffer, dataHead: DataHead, meta: Meta): Matrix = {
    assert(dataHead.shape.length == 2)
    assert(dataHead.sparseDim == 2 && dataHead.denseDim == 0) // matrix (both dim is sparse)

    val matrixId = meta.getMatrixContext.getMatrixId

    val bytes = buf.array()
    var start: Int = buf.position()
    val rowBuf = ByteBuffer.wrap(bytes, start, dataHead.nnz * 8)
    rowBuf.order(ByteOrder.LITTLE_ENDIAN)
    start += dataHead.nnz * 8

    val colBuf = ByteBuffer.wrap(bytes, start, dataHead.nnz * 8)
    colBuf.order(ByteOrder.LITTLE_ENDIAN)
    start += dataHead.nnz * 8

    val valBuf = ByteBuffer.wrap(bytes, start, bytes.length - start)
    valBuf.order(ByteOrder.LITTLE_ENDIAN)

    var lastRowId: Long = -1L
    val csrRow = new ListBuffer[Int]()
    val mkPos = rowBuf.position()
    (0 until dataHead.nnz).foreach { i =>
      val rowId = rowBuf.getLong()
      if (lastRowId != rowId) {
        lastRowId = rowId
        csrRow.append(i)
      }
    }
    csrRow.append(dataHead.nnz)
    val rowIdx = csrRow.toArray

    rowBuf.position(mkPos)
    val vectors: Array[Vector] = (0 until rowIdx.length-1).toArray.map { i =>
      val len = rowIdx(i + 1) - rowIdx(i)
      val rowId = rowBuf.getLong().toInt
      rowBuf.position(rowBuf.position() + (len-1) * 8)

      getSparseVector(matrixId, rowId, len, dataHead.shape(1), dataHead, meta, colBuf, valBuf)
    }

    buf.position(valBuf.position())
    Utils.vectorArray2Matrix(vectors)
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
    val dataHead = DataHead.fromBuffer(buf)
    if (dataHead.shape.length >= 1) {
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
      println(dataHead.shape.length)
      null
    }
  }

  private def dummyVectorFromBuffer(buf: ByteBuffer, dataHead: DataHead, meta: Meta): Vector = {
    assert(dataHead.sparseDim == 1 && dataHead.denseDim == -1 )  // vector, dummy
    val oldOrder = buf.order()
    buf.order(ByteOrder.LITTLE_ENDIAN)

    val dim  = meta.shape(0)
    val matrixId = meta.getMatrixContext.getMatrixId
    val vector = if (isIntKey(meta)) {
      val values = new Array[Int](dataHead.nnz)
      var i = 0
      while (i < dataHead.nnz) {
        values(i) = buf.getLong.toInt
        i += 1
      }
      VFactory.intDummyVector(matrixId, 0, 0, dim.toInt, values)
    } else {
      val values = new Array[Long](dataHead.nnz)
      var i = 0
      while (i < dataHead.nnz) {
        values(i) = buf.getLong
        i += 1
      }
      VFactory.longDummyVector(matrixId, 0, 0, dim, values)
    }

    buf.order(oldOrder)
    vector
  }

  private def sparseVectorFromBuffer(buf: ByteBuffer, dataHead: DataHead, meta: Meta): Vector = {
    assert(dataHead.sparseDim == 1 && dataHead.denseDim == 0)  // vector, sparse

    val matrixId = meta.getMatrixContext.getMatrixId
    val bytes = buf.array()
    var start: Int = buf.position()
    val colBuf = ByteBuffer.wrap(bytes, start, dataHead.nnz * 8)
    colBuf.order(ByteOrder.LITTLE_ENDIAN)
    start += dataHead.nnz * 8

    val valBuf = ByteBuffer.wrap(bytes, start, bytes.length - start)
    valBuf.order(ByteOrder.LITTLE_ENDIAN)

    val vector = getSparseVector(matrixId, 0, dataHead.nnz, meta.shape(0), dataHead, meta, colBuf, valBuf)

    start += valBuf.position()
    buf.position(start)
    vector
  }

  private def denseVectorFromBuffer(buf: ByteBuffer, dataHead: DataHead, meta: Meta): Vector = {
    assert(dataHead.sparseDim == -1 && dataHead.denseDim > 0)  // vector, sparse
    val oldOrder = buf.order()
    buf.order(ByteOrder.LITTLE_ENDIAN)

    val matrixId = meta.getMatrixContext.getMatrixId
    val vector = getDenseVector(matrixId, 0, dataHead.nnz, dataHead, buf)

    buf.order(oldOrder)
    vector
  }

  def indicesFromBuffer(buf: ByteBuffer, dataHead: DataHead, meta: Meta): Vector = {
    assert(meta.getMatrixContext.getRowType.isSparse)

    val bytes = buf.array()
    var start = buf.position()
    val idxBuf = if (dataHead.sparseDim == 1) {
      ByteBuffer.wrap(bytes, start, dataHead.nnz * 8)
    } else if (dataHead.sparseDim == 2) {
      start += dataHead.nnz * 8
      ByteBuffer.wrap(bytes, start, dataHead.nnz * 8)
    } else {
      throw new Exception("indicesFromBuffer not supported!")
    }

    idxBuf.order(ByteOrder.LITTLE_ENDIAN)

    if (isIntKey(meta)) {
      val hashSet = new IntOpenHashSet(dataHead.nnz)
      var i = 0
      while (i < dataHead.nnz) {
        hashSet.add(idxBuf.getLong.toInt)
        i += 1
      }

      val data = hashSet.toIntArray
      // util.Arrays.sort(data)
      VFactory.denseIntVector(data)
    } else {
      val hashSet = new LongOpenHashSet(dataHead.nnz)
      var i = 0
      while (i < dataHead.nnz) {
        hashSet.add(idxBuf.getLong)
        i += 1
      }

      val data = hashSet.toLongArray
      // util.Arrays.sort(data)
      VFactory.denseLongVector(data)
    }
  }

}
