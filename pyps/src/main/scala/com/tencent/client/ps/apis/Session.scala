package com.tencent.client.ps.apis

import com.tencent.client.ps.tensor.{Tensor, TensorLike}
import com.tencent.client.ps.variable.Variable
import org.apache.spark.internal.Logging

import scala.collection.mutable

class Session(val pid: Int) extends Logging {
  private val tsMap = new mutable.HashMap[String, Tensor]()
  private val varMap = new mutable.HashMap[String, Variable]()

  private var chief: Boolean = false

  def isChief: Boolean = chief

  def setChief(flag: Boolean): this.type = {
    chief = flag
    this
  }

  def hasTensor(name: String): Boolean = {
    tsMap.contains(name)
  }

  def hasTensor(matId: Int): Boolean = {
    val tensor = tsMap.values.collectFirst { case ts: Tensor if ts.getMatClient.getMatrixId == matId => ts }
    tensor.nonEmpty
  }

  def getTensor(name: String): Tensor = {
    tsMap.getOrElse(name, null.asInstanceOf[Tensor])
  }

  def getTensor(matId: Int): Tensor = {
    val tensor = tsMap.values.collectFirst { case ts: Tensor if ts.getMatClient.getMatrixId == matId => ts }
    if (tensor.nonEmpty) {
      tensor.get
    } else {
      throw new Exception("matrix no exist, please create first!")
    }
  }

  def hasVariable(name: String): Boolean = {
    varMap.contains(name)
  }

  def hasVariable(matId: Int): Boolean = {
    val variable = varMap.values.collectFirst { case va: Variable if va.getMatClient.getMatrixId == matId => va }
    variable.nonEmpty
  }

  def getVariable(name: String): Variable = {
    varMap.getOrElse(name, null.asInstanceOf[Variable])
  }

  def getVariable(matId: Int): Variable = {
    val variable = varMap.values.collectFirst { case va: Variable if va.getMatClient.getMatrixId == matId => va }

    if (variable.nonEmpty) {
      variable.get
    } else {
      throw new Exception("matrix no exist, please create first!")
    }
  }

  def get(name: String): TensorLike = {
    if (tsMap.contains(name)) {
      tsMap(name)
    } else if (varMap.contains(name)) {
      varMap(name)
    } else {
      throw new Exception("matrix no exist, please create first!")
    }
  }

  def get(matId: Int): TensorLike = {
    val tensor = tsMap.values.collectFirst { case ts: Tensor if ts.getMatClient.getMatrixId == matId => ts }
    if (tensor.nonEmpty) {
      tensor.get
    } else {
      val variable = varMap.values.collectFirst { case va: Variable if va.getMatClient.getMatrixId == matId => va }
      if (variable.nonEmpty) {
        variable.get
      } else {
        throw new Exception("matrix no exist, please create first!")
      }
    }
  }

  def put(name: String, value: TensorLike): this.type = {
    value match {
      case ts: Tensor => tsMap.put(name, ts)
      case va: Variable => varMap.put(name, va)
    }

    this
  }

  def contains(name: String): Boolean = {
    if (tsMap.contains(name)) {
      true
    } else {
      varMap.contains(name)
    }
  }

  def contains(matId: Int): Boolean = {
    val tensor = tsMap.values.collectFirst { case ts: Tensor if ts.getMatClient.getMatrixId == matId => ts }
    if (tensor.nonEmpty) {
      true
    } else {
      val variable = varMap.values.collectFirst { case va: Variable if va.getMatClient.getMatrixId == matId => va }
      variable.nonEmpty
    }
  }

  def remove(name: String): Option[TensorLike] = {
    if (tsMap.contains(name)) {
      tsMap.remove(name)
    } else if (varMap.contains(name)) {
      varMap.remove(name)
    } else {
      logInfo(s"$name is not exist in Session")
      None
    }
  }

  def remove(matId: Int): Option[TensorLike] = {
    val tensor = tsMap.values.collectFirst { case ts: Tensor if ts.getMatClient.getMatrixId == matId => ts }
    if (tensor.nonEmpty) {
      tsMap.remove(tensor.get.name)
    } else {
      val variable = varMap.values.collectFirst { case va: Variable if va.getMatClient.getMatrixId == matId => va }
      if (variable.nonEmpty) {
        varMap.remove(variable.get.name)
      } else {
        logInfo(s"$matId is not exist in Session")
        None
      }
    }
  }

  def clear(): Unit = {
    tsMap.clear()
    varMap.clear()
  }
}
