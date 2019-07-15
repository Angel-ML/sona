package com.tencent.angel.apiserver

import java.nio.ByteBuffer
import java.util

class Require(val pid: Long, val funcId: Long, val matId: Int, val epoch: Int, val batch: Int, val msgLen: Int, objectId: Array[Byte]) {
  private lazy val reqBytes: Array[Byte] = {
    val bytes = new Array[Byte](Require.bufferLen)
    val buf = ByteBuffer.wrap(bytes)
    put2Buffer(buf)

    bytes
  }

  def hasObjectId: Boolean = {
    objectId != null
  }

  def getObjectId: Array[Byte] = {
    objectId
  }

  def put2Buffer(buf: ByteBuffer): Unit = {
    assert(buf.remaining() >= Require.bufferLen)

    val mkPos = buf.position()
    buf.putLong(pid)
    buf.putLong(funcId)
    buf.putInt(matId)
    buf.putInt(epoch)
    buf.putInt(batch)
    buf.putInt(msgLen)

    if (msgLen > 0) {
      buf.put(objectId)
    }

    if (buf.position() - mkPos < Require.bufferLen) {
      val paddingLen = Require.bufferLen - buf.position() + mkPos
      (0 until paddingLen).foreach{_ => buf.put(0.toByte)}
    }
  }

  def toBuffer: ByteBuffer = {
    val buf = ByteBuffer.wrap(reqBytes)
    buf.asReadOnlyBuffer()
  }

  def toBytes: Array[Byte] = reqBytes

  override def toString: String = {
    if (objectId != null) {
      s"{pid=$pid, funcId=$funcId, matId=$matId, epoch=$epoch, batch=$batch, objectId=${new String(objectId)}}"
    } else {
      s"{pid=$pid, funcId=$funcId, matId=$matId, epoch=$epoch, batch=$batch, objectId=null}"
    }
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case req: Require => util.Arrays.equals(reqBytes, req.reqBytes)
      case _ => false
    }
  }

  override def hashCode(): Int = {
    util.Arrays.hashCode(reqBytes)
  }
}

object Require {
  val bufferLen = 128 // padding zeros

  def apply(pid: Long, funcId: Long, matId: Int, epoch: Int, batch: Int, msgLen: Int, objectId: Array[Byte]): Require = {
    new Require(pid, funcId, matId, epoch, batch, msgLen, objectId)
  }

  def unapply(req: Require): Option[(Long, Long, Int, Int, Int, Int, Array[Byte])] = {
    if (req == null) {
      None
    } else {
      Some(req.pid, req.funcId, req.matId, req.epoch, req.batch, req.msgLen, req.getObjectId)
    }
  }

  def fromBuffer(buf: ByteBuffer): Require = {
    val pid = buf.getLong
    val funcId = buf.getLong
    val matId = buf.getInt
    val epoch = buf.getInt
    val batch = buf.getInt
    val msgLen = buf.getInt()

    if (msgLen > 0) {
      val objectId = new Array[Byte](20)
      buf.get(objectId)
      new Require(pid, funcId, matId, epoch, batch, msgLen, objectId)
    } else { // no objectId
      new Require(pid, funcId, matId, epoch, batch, 0,null)
    }
  }

  class ReqKey(val pid: Long, matId: Int, epoch: Int, batch: Int) {
    override def toString: String = {
      s"{matId=$matId, epoch=$epoch, batch=$batch}"
    }

    override def equals(obj: Any): Boolean = {
      obj match {
        case o : ReqKey => matId == o.matId && epoch == o.epoch && batch == o.batch
        case _ => false
      }
    }

    override def hashCode(): Int = {
      util.Arrays.hashCode(Array[Int](matId, epoch, batch))
    }
  }

  implicit def toReqKey(req: Require): ReqKey = {
    new ReqKey(req.pid, req.matId, req.epoch, req.batch)
  }
}

