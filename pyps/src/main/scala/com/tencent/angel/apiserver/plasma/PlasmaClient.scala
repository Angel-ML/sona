package com.tencent.angel.apiserver.plasma

import java.nio.ByteBuffer
import java.util.Random

import com.tencent.angel.apiserver.plasma.exceptions.{DuplicateObjectException, PlasmaOutOfMemoryException}
import com.tencent.angel.ml.servingmath2.matrix.Matrix
import com.tencent.angel.ml.servingmath2.utils.LabeledData



class PlasmaClient(storeSocketName: String, managerSocketName: String, releaseDelay: Int) {
  private val conn = PlasmaClientJNI.connect(storeSocketName, managerSocketName, releaseDelay)

  @throws[DuplicateObjectException]
  @throws[PlasmaOutOfMemoryException]
  def create(objectId: Array[Byte], buf: ByteBuffer): Unit = {
    val tmpBuf = PlasmaClientJNI.create(conn, objectId, buf.remaining(), null)
    tmpBuf.put(buf)
    PlasmaClientJNI.seal(conn, objectId)
    PlasmaClientJNI.release(conn, objectId)
  }

  @throws[DuplicateObjectException]
  @throws[PlasmaOutOfMemoryException]
  def create(objectId: Array[Byte], buf: ByteBuffer, metadata: Array[Byte]): Unit = {
    val tmpBuf = PlasmaClientJNI.create(conn, objectId, buf.remaining(), metadata)
    tmpBuf.put(buf)
    PlasmaClientJNI.seal(conn, objectId)
    PlasmaClientJNI.release(conn, objectId)
  }

  @throws[DuplicateObjectException]
  @throws[PlasmaOutOfMemoryException]
  def create(objectId: Array[Byte], buf: ByteBuffer, metadata: ByteBuffer): Unit = {
    val meta = new Array[Byte](metadata.remaining())
    metadata.get(meta)
    create(objectId, buf, metadata)
  }

  @throws[DuplicateObjectException]
  @throws[PlasmaOutOfMemoryException]
  def put(objectId: Array[Byte], mat: Matrix): Array[Byte] = {
    val buf = PlasmaClientJNI.create(conn, objectId, value.length, metadata)

    PlasmaClientJNI.seal(conn, objectId)
    PlasmaClientJNI.release(conn, objectId)

    objectId
  }

  def get(objectId: Array[Byte]): Matrix = {

  }

  def putBatch(data: Array[LabeledData]): Array[Byte] = {

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
