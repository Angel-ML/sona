package com.tencent.angel.common

import java.nio.{ByteBuffer, ByteOrder}


class DataHead(val sparseDim: Int, val denseDim: Int, val shape: Array[Long], val nnz: Int, val dtype: Int, val length: Int) {
  assert(sparseDim + denseDim <= 8)

  def toBytes: Array[Byte] = {
    val bytes = new Array[Byte](DataHead.headLen)
    val buf = ByteBuffer.wrap(bytes)
    put2Buf(buf)

    bytes
  }

  def put2Buf(buf: ByteBuffer): this.type = {
    assert(buf.capacity() - buf.position() >= DataHead.headLen)
    val oldOrder = buf.order()
    buf.order(ByteOrder.LITTLE_ENDIAN)

    val start = buf.position()
    buf.putInt(sparseDim)
    buf.putInt(denseDim)

    val mkPos = buf.position()
    shape.foreach(sp => buf.putLong(sp))
    var i = buf.position()
    while (i < mkPos + 64) { // padding
      buf.put(0.asInstanceOf[Byte])
      i += 1
    }
    buf.putInt(nnz)
    buf.putInt(dtype)
    buf.putInt(length)
    i = buf.position()
    while (i < start + DataHead.headLen) { // padding
      buf.put(0.asInstanceOf[Byte])
      i += 1
    }



    buf.order(oldOrder)
    this
  }
}

object DataHead {
  val headLen = 128
  val DT_INT = 1
  val DT_LONG = 2
  val DT_FLOAT = 4
  val DT_DOUBLE = 8

  def fromBuffer(buf: ByteBuffer): DataHead = {
    assert(buf.remaining() >= headLen)
    val oldOrder = buf.order()
    buf.order(ByteOrder.LITTLE_ENDIAN)
    val start = buf.position()
    val sparseDim = buf.getInt // pos = 4
    val denseDim = buf.getInt // pos = 4
    val dim = if (sparseDim == -1 && denseDim > 0) { // dense
      denseDim
    } else if (sparseDim > 0 && denseDim == -1) { // dummy
      sparseDim
    } else { // sparse
      sparseDim + denseDim
    }

    val shape = new Array[Long](dim)
    val mkPos = buf.position()
    (0 until dim).foreach { i => shape(i) = buf.getLong() }
    buf.position(mkPos + 64)

    val nnz = buf.getInt
    val dtype = buf.getInt
    val length = buf.getInt

    buf.position(start + headLen)
    buf.order(oldOrder)
    println(sparseDim,denseDim,shape,nnz,dtype,length)
    new DataHead(sparseDim, denseDim, shape, nnz, dtype, length)
  }
}
