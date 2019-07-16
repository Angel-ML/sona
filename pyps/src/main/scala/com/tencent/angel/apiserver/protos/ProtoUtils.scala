package com.tencent.angel.apiserver.protos

import java.util
import java.lang.{Long => JLong}

object ProtoUtils {
  def toArray(list: util.List[JLong]): Array[Long] = {
    val res = new Array[Long](list.size())
    val iter = list.iterator()

    var i = 0
    while(iter.hasNext) {
      val value = iter.next()
      res(i) = value
      i += 1
    }

    res
  }


}
