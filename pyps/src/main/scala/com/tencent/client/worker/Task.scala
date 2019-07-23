package com.tencent.client.worker


import java.util.logging.Logger
import com.tencent.client.common.protos.AsyncModel
import com.tencent.client.worker.ps.tensor.{Tensor, TensorLike}
import com.tencent.client.worker.ps.variable.Variable

import scala.collection.mutable

case class TaskInfo(taskId: Long, numTask: Int, clock: Map[Long, Int])

class Task(masterStub: MStub, val isChief: Boolean) {
  private val logger = Logger.getLogger(classOf[Task].getSimpleName)

  var taskId: Long = 0
  var numTask: Int = 0

  private var clock: Map[Long, Int] = _
  private var currClock: Int = 0

  private val tsMap = new mutable.HashMap[String, Tensor]()
  private val varMap = new mutable.HashMap[String, Variable]()

  private[this] var _batchSize: Int = 0

  def batchSize: Int = _batchSize

  def batchSize_=(value: Int): Unit = {
    _batchSize = value
  }

  private[this] var _epoch: Int = 0

  def epoch: Int = _epoch

  def epoch_=(value: Int): Unit = {
    _epoch = value
  }

  private[this] var _batch: Int = 0

  def batch: Int = _batch

  def batch_=(value: Int): Unit = {
    _batch = value
  }

  def register(): Unit = {
    val taskInfo = masterStub.registerTask
    taskId = taskInfo.taskId
    numTask = taskInfo.numTask
    clock = taskInfo.clock
    currClock = taskInfo.clock(taskId)
  }

  def sync(): Unit = {
    val tmpClock = currClock + 1
    var clockMap = masterStub.clock(taskId, tmpClock, batchSize)

    if (masterStub.asyncModel == AsyncModel.BSP) {
      while (clockMap.values.min < tmpClock) {
        println(s"${Thread.currentThread().getId}: $taskId> minClock:${clockMap.values.min} -- thisClock:$tmpClock")
        Thread.sleep(100)
        clockMap = masterStub.getClockMap(taskId)
      }
    } else if (masterStub.asyncModel == AsyncModel.SSP) {
      while (clockMap.values.max - tmpClock < 4) {
        Thread.sleep(100)
        clockMap = masterStub.getClockMap(taskId)
      }
    } else { // AsyncModel.ASP
      // do nothing
    }

    currClock = clockMap(taskId)
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
      logger.info(s"$name is not exist in Session")
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
        logger.info(s"$matId is not exist in Session")
        None
      }
    }
  }

  def clear(): Unit = {
    tsMap.clear()
    varMap.clear()
  }
}
