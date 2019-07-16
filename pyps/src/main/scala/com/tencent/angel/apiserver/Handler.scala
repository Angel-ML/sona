package com.tencent.angel.apiserver

import java.nio.ByteBuffer
import java.util
import com.tencent.angel.apiserver.protos.MSGProtos._
import com.tencent.angel.apiserver.plasma.PlasmaClient
import scala.reflect.runtime.universe._

class Handler(client: PlasmaClient, totalMemInByte: Long) {
  private val funcMap = new util.HashMap[Long, Request => Response]()
  private val handlerMap = new util.HashMap[Int, util.ArrayList[Long]]()

  def register[T: TypeTag](obj: T): this.type = synchronized {
    val tpe: Type = typeOf[T]
    val symbol: Symbol = tpe.typeSymbol
    val annotation: Annotation = symbol.annotations.head
    val Apply(_, Literal(Constant(handlerId: Int)) :: Nil) = annotation.tree

    val list = new util.ArrayList[Long]()
    handlerMap.put(handlerId, list)

    obj.getClass.getMethods.map { method =>
      if (method.getParameterCount == 1 && method.getParameterTypes.head == classOf[Request] &&
        method.getReturnType == classOf[Response]) {
        val func = (req: Request) => method.invoke(obj, req).asInstanceOf[Response]

        val methodAnnotation = tpe.decl(TermName(method.getName)).annotations.head
        val Apply(_, Literal(Constant(funcId: Int)) :: Nil) = methodAnnotation.tree

        val buf = ByteBuffer.allocate(8)
        buf.putInt(handlerId)
        buf.putInt(funcId)
        list.add(funcId)

        buf.flip()
        funcMap.put(buf.getLong, func)
      }
    }


    this
  }

  def unregister[T: TypeTag](obj: T): this.type = synchronized {
    val tpe: Type = typeOf[T]
    val symbol: Symbol = tpe.typeSymbol
    val annotation: Annotation = symbol.annotations.head
    val Apply(_, Literal(Constant(handlerId: Int)) :: Nil) = annotation.tree

    if (handlerMap.containsKey(handlerId)) {
      val iter = handlerMap.get(handlerId).iterator()
      while (iter.hasNext) {
        val funcId = iter.next()
        if (funcMap.containsKey(funcId)) {
          funcMap.remove(funcId)
        }
      }

      handlerMap.remove(handlerId)
    }

    this
  }

  def handle(req: Request): Response = {
    if (funcMap.containsKey(req.getFuncId)) {
      funcMap.get(req.getFuncId)(req)
    } else {
      throw new Exception("method not found!")
    }
  }
}
