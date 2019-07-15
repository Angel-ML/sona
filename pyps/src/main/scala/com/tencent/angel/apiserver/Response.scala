package com.tencent.angel.apiserver

import java.nio.ByteBuffer
import java.util

class Response(val pid: Long, val funcId: Long, val sharedFlag: Int, val msgLen: Int, objectId: Array[Byte]) {
  private lazy val respBytes: Array[Byte] = {
    val bytes = new Array[Byte](Response.bufferLen)
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
    assert(buf.remaining() >= Response.bufferLen)

    val mkPos = buf.position()
    buf.putLong(pid)
    buf.putLong(funcId)
    buf.putInt(sharedFlag)

    assert(msgLen >= 0)
    buf.putInt(msgLen)
    if (msgLen > 0) {
      buf.put(objectId)
    }

    if (buf.position() - mkPos < Request.bufferLen) {
      val paddingLen = Request.bufferLen - buf.position() + mkPos
      (0 until paddingLen).foreach{_ => buf.put(0.toByte)}
    }
  }

  def toBuffer: ByteBuffer = {
    val buf = ByteBuffer.wrap(respBytes)
    buf.asReadOnlyBuffer()
  }

  def toBytes: Array[Byte] = respBytes

  override def toString: String = {
    if (objectId != null) {
      s"{pid=$pid, funcId=$funcId, sharedFlag=$sharedFlag, msgLen=$msgLen, objectId=${new String(objectId)}"
    } else {
      s"{pid=$pid, funcId=$funcId, sharedFlag=$sharedFlag, msgLen=$msgLen, objectId=null}"
    }
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case req: Response => util.Arrays.equals(respBytes, req.respBytes)
      case _ => false
    }
  }

  override def hashCode(): Int = {
    util.Arrays.hashCode(respBytes)
  }
}

object Response{
  val bufferLen: Int = 64 // padding zeros

  def apply(pid: Long, funcId: Long, sharedFlag: Int, msgLen: Int, objectId: Array[Byte]): Response = {
    new Response(pid, funcId, sharedFlag, msgLen, objectId)
  }

  def unapply(resp: Response): Option[(Long, Long, Int, Int, Array[Byte])] = {
    if (resp == null) {
      None
    } else {
      Some(resp.pid, resp.funcId, resp.sharedFlag, resp.msgLen, resp.getObjectId)
    }
  }

  def fromBuffer(buf: ByteBuffer): Response = {
    val pid = buf.getLong
    val funcId = buf.getLong
    val sharedFlag = buf.getInt
    val msgLen = buf.getInt

    if (msgLen > 0) {
      val objectId = new Array[Byte](20)
      buf.get(objectId)
      new Response(pid, funcId, sharedFlag, msgLen, objectId)
    } else { // no objectId
      new Response(pid, funcId, sharedFlag, 0,null)
    }
  }
}
