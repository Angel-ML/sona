package com.tencent.angel.apiserver

import java.util

import com.tencent.angel.apiserver.plasma.PlasmaClient

import scala.collection.mutable

class MemMsger(client: PlasmaClient, totalMemInByte: Long) extends Runnable{
  private val shared = new mutable.HashMap[Request.ReqKey, Response]()
  private val unshared = new mutable.HashMap[Request, Response]()
  private val status = new mutable.HashMap[Long, MemMsger.Status]()
  private var usedMemInByte: Long = 0

  private def delete(req: Request): this.type = {
    if (req.hasObjectId && client.contains(req.getObjectId)) {
      client.delete(req.getObjectId)
    }

    this
  }

  private def delete(resp: Response): this.type = {
    if (resp.hasObjectId && client.contains(resp.getObjectId)) {
      client.delete(resp.getObjectId)
    }

    this
  }

  def hasResponse(req: Request): Boolean = synchronized {
    shared.contains(req)
  }

  def get(req: Request): Response = synchronized {
    if (shared.contains(req)) {
      val temp = shared(req)
      assert(temp.funcId == req.funcId)
      Response(req.pid, temp.funcId, temp.sharedFlag, temp.msgLen, temp.getObjectId)
    } else {
      null
    }
  }

  def put(req: Request, resp: Response): this.type = synchronized {
    usedMemInByte += req.msgLen
    usedMemInByte += resp.msgLen

    if (resp.sharedFlag != 0) {
      assert(!req.hasObjectId)
      // 1. add to shared
      if (!shared.contains(req)) {
        shared.put(req, resp)
      }
    } else {
      if (!unshared.contains(req)) {
        unshared.put(req, resp)
      }
    }

    if (status.contains(req.pid)) {
      val state = status(req.pid)
      state.epoch = req.epoch
      state.batch = req.batch
    } else {
      status(req.pid) = new MemMsger.Status(req.pid, req.epoch, req.batch)
    }

    this
  }

  override def run(): Unit = {
    try {
      while (!Thread.currentThread().isInterrupted) {
        while(true) {
          if (1.0 * usedMemInByte / totalMemInByte > 0.8) {

          }
        }
      }
    } catch {
      case e: InterruptedException =>
        e.printStackTrace()
        throw e
    }
  }
}

object MemMsger {
  class Status(val pid: Long, var epoch: Int, var batch: Int)
}
