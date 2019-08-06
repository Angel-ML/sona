package org.apache.spark.angel.graph.line2

import java.io.{DataInputStream, DataOutputStream}

import com.tencent.angel.model.output.format.{ComplexRowFormat, IndexAndElement}
import com.tencent.angel.ps.storage.vector.element.IElement
import org.apache.hadoop.conf.Configuration

class TextLINEModelOutputFormat(conf:Configuration) extends ComplexRowFormat(conf) {
  val featSep = conf.get("line.feature.sep", " ")
  val keyValueSep = conf.get("line.keyvalue.sep", ":")

  override def load(input: DataInputStream): IndexAndElement = {
    val line = input.readLine()
    val indexAndElement = new IndexAndElement
    val keyValues = line.split(keyValueSep)

    if(featSep.equals(keyValueSep)) {
      indexAndElement.index = keyValues(0).toLong
      val feats = new Array[Float](keyValues.length - 1)
      (1 until keyValues.length).foreach(i => feats(i - 1) = keyValues(i).toFloat)
      indexAndElement.element = new LINENode(feats, null)
    } else {
      indexAndElement.index = keyValues(0).toLong
      indexAndElement.element = new LINENode(keyValues(1).split(featSep).map(f => f.toFloat), null)
    }
    indexAndElement
  }

  override def save(key: Long, value: IElement, output: DataOutputStream): Unit = {
    val sb = new StringBuilder

    // Write node id
    sb.append(key)
    sb.append(keyValueSep)

    // Write feats
    val feats = value.asInstanceOf[LINENode].getInputFeats

    var index = 0
    val len = feats.length
    feats.foreach(f => {
      if(index < len - 1) {
        sb.append(f).append(featSep)
      } else {
        sb.append(f).append("\n")
      }
      index += 1
    })

    output.writeBytes(sb.toString())
  }
}
