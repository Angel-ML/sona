package com.tencent.angel.apiserver

import java.nio.ByteBuffer
import java.util

import com.tencent.angel.apiserver.plasma.PlasmaClient

class Handler(client: PlasmaClient) {
  private val funcMap = new util.HashMap[Long, Require => Response]()
  private val handlerMap = new util.HashMap[Int, util.ArrayList[Long]]()

  private val memMsger = new MemMsger(client)
  private val memMsgerThread = new Thread(memMsger)

  def startMemMsger():Unit = {
    memMsgerThread.start()
    memMsgerThread.setDaemon(true)
  }

  def stopMemMsger():Unit = {
    memMsgerThread.interrupt()
  }

  def register(obj: AnyRef): this.type = synchronized {
    val handlerId = obj.getClass.getAnnotation(classOf[HandlerId])

    if (handlerId != null) {
      val list = new util.ArrayList[Long]()
      handlerMap.put(handlerId.id, list)

      obj.getClass.getMethods.map{ method =>
        if (method.getParameterCount == 1 && method.getParameterTypes.head == classOf[Require] &&
          method.getReturnType == classOf[Response]) {
          val func = (req: Require) => method.invoke(obj, req).asInstanceOf[Response]

          val funcId = method.getAnnotation(classOf[FuncId])

          val buf = ByteBuffer.allocate(8)
          buf.putInt(handlerId.id)
          buf.putInt(funcId.id)
          list.add(funcId.id)

          buf.flip()
          funcMap.put(buf.getLong, func)
        }
      }
    }

    this
  }

  def unregister(obj: AnyRef): this.type = synchronized {
    val handlerId = obj.getClass.getAnnotation(classOf[HandlerId])

    if (handlerId != null && handlerMap.containsKey(handlerId.id)) {
      val iter = handlerMap.get(handlerId.id).iterator()
      while (iter.hasNext) {
        val funcId = iter.next()
        if (funcMap.containsKey(funcId)) {
          funcMap.remove(funcId)
        }
      }

      handlerMap.remove(handlerId.id)
    }

    this
  }

  def handle(req: Require): Response = {
    if (memMsger.hasResponse(req)) {
      memMsger.get(req)
    } else {
      if (handlerMap.containsKey(req.funcId)) {
        val resp: Response = handlerMap(req.funcId)(req)
        memMsger.put(req, resp)
        resp
      } else {
        throw new Exception("method not found!")
      }
    }
  }
}
