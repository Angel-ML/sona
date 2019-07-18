package com.tencent.client.apiserver.plasma

import java.nio.ByteBuffer
import java.util
import java.util.{ArrayList, List, Random}

import com.tencent.client.apiserver.plasma.exceptions.{DuplicateObjectException, PlasmaOutOfMemoryException}
import com.tencent.client.common.{DataHead, Deserializer, Meta, Serializer}
import com.tencent.angel.ml.servingmath2.vector._
import com.tencent.angel.ml.servingmath2.matrix._
import com.tencent.angel.ml.servingmath2.utils.LabeledData



class PlasmaClient(storeSocketName: String, managerSocketName: String, releaseDelay: Int) {
  private lazy val conn = PlasmaClientJNI.connect(storeSocketName, managerSocketName, releaseDelay)

  @throws[DuplicateObjectException]
  @throws[PlasmaOutOfMemoryException]
  def put(objectId: Array[Byte], buf: ByteBuffer): Unit = {
    val tmpBuf = PlasmaClientJNI.create(conn, objectId, buf.remaining(), null)
    tmpBuf.put(buf)
    PlasmaClientJNI.seal(conn, objectId)
    PlasmaClientJNI.release(conn, objectId)
  }

  @throws[DuplicateObjectException]
  @throws[PlasmaOutOfMemoryException]
  def put(objectId: Array[Byte], buf: ByteBuffer, metadata: Array[Byte]): Unit = {
    val tmpBuf = PlasmaClientJNI.create(conn, objectId, buf.remaining(), metadata)
    tmpBuf.put(buf)
    PlasmaClientJNI.seal(conn, objectId)
    PlasmaClientJNI.release(conn, objectId)
  }

  @throws[DuplicateObjectException]
  @throws[PlasmaOutOfMemoryException]
  def put(objectId: Array[Byte], buf: ByteBuffer, metadata: ByteBuffer): Unit = {
    val meta = new Array[Byte](metadata.remaining())
    metadata.get(meta)
    put(objectId, buf, metadata)
  }

  @throws[DuplicateObjectException]
  @throws[PlasmaOutOfMemoryException]
  def put(objectId: Array[Byte], mat: Matrix, meta: Meta): Array[Byte] = {
    val dataHead: DataHead = Serializer.getDataHeadFromMatrix(mat, meta)
    val buf = PlasmaClientJNI.create(conn, objectId, DataHead.headLen + dataHead.length, null)

    Serializer.putMatrix(buf, mat, meta, dataHead)

    PlasmaClientJNI.seal(conn, objectId)
    PlasmaClientJNI.release(conn, objectId)

    objectId
  }

  @throws[DuplicateObjectException]
  @throws[PlasmaOutOfMemoryException]
  def put(objectId: Array[Byte], vec: Vector, meta: Meta): Array[Byte] = {
    val dataHead: DataHead = Serializer.getDataHeadFromVector(vec, meta)
    val buf = PlasmaClientJNI.create(conn, objectId, DataHead.headLen + dataHead.length, null)

    Serializer.putVector(buf, vec, meta, dataHead)

    PlasmaClientJNI.seal(conn, objectId)
    PlasmaClientJNI.release(conn, objectId)

    objectId
  }

  def getMatrix(objectId: Array[Byte], meta: Meta, timeoutMs: Int): Matrix = {
    val bufs = PlasmaClientJNI.get(conn, Array[Array[Byte]](objectId), timeoutMs)
    assert(bufs.length == 1)

    val dataBuf = bufs(0)(0)
    val metaBuf = bufs(0)(1)

    Deserializer.matrixFromBuffer(dataBuf, meta)
  }

  def getVector(objectId: Array[Byte], meta: Meta, timeoutMs: Int): Vector = {
    val bufs = PlasmaClientJNI.get(conn, Array[Array[Byte]](objectId), timeoutMs)
    assert(bufs.length == 1)

    val dataBuf = bufs(0)(0)
    val metaBuf = bufs(0)(1)

    Deserializer.vectorFromBuffer(dataBuf, meta)
  }

  def getBuffer(objectId: Array[Byte], timeoutMs: Int): ByteBuffer = {
    val bufs = PlasmaClientJNI.get(conn, Array[Array[Byte]](objectId), timeoutMs)
    assert(bufs.length == 1)

    bufs(0)(0)
  }

  def putBatch(data: Array[LabeledData]): Array[Byte] = {
    null
  }

  def seal(objectId: Array[Byte]): Unit = {
    PlasmaClientJNI.seal(conn, objectId)
  }

  def release(objectId: Array[Byte]): Unit = {
    PlasmaClientJNI.release(conn, objectId)
  }

  def delete(objectId: Array[Byte]): Unit = {
    PlasmaClientJNI.delete(conn, objectId)
  }

  def contains(objectId: Array[Byte]):Boolean = {
    PlasmaClientJNI.contains(conn, objectId)
  }
}

object PlasmaClient {
  private val rand = new Random()

  def getObjectId(pid: Long, matId: Int, epoch: Int, batch: Int): Array[Byte] = {
    val bytes = new Array[Byte](20)
    val buf = ByteBuffer.wrap(bytes)

    buf.putLong(pid)
    buf.putInt(matId)
    buf.putInt(epoch)
    buf.putInt(batch)

    bytes
  }

  def randomObjectId: Array[Byte] = {
    val bytes = new Array[Byte](20)
    rand.nextBytes(bytes)
    bytes
  }
}
