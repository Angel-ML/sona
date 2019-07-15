package com.tencent.angel.ps.updater

import com.tencent.angel.ps.variable.{Updater, Variable}

trait Optimizer extends Updater with Serializable {
  var lr: Double
  val epsilon: Double = 1e-10

  protected var regL1Param: Double = 0.0
  protected var regL2Param: Double = 0.0

  def setLR(lr: Double): this.type = {
    this.lr = lr
    this
  }

  def setRegL1Param(regParam: Double): this.type = {
    this.regL1Param = regParam
    this
  }

  def setRegL2Param(regParam: Double): this.type = {
    this.regL2Param = regParam
    this
  }

  def getLR: Double = this.lr

  def getRegL1Param: Double = this.regL1Param

  def getRegL2Param: Double = this.regL2Param

  def getNumInnerRows(variable: Variable): Int = {
    variable.getMeta.getMatrixContext.getRowNum / (numSlot + 1)
  }
}


object Optimizer {
  private def getDouble(map: Map[String, String])(key: String, default: Double): Double = {
    try {
      map.getOrElse(key, default.toString).toDouble
    } catch {
      case _: Exception => default
    }
  }

  def get(params: Map[String, String]): Optimizer = {
    val name = params.getOrElse("name", "Momentum")
    val lr = params.getOrElse[Double]("lr", 0.0001)
    val getD = getDouble(params)

    val opt = if (name.equalsIgnoreCase("AdaDelta")) {
      // AdaDelta(lr: Double, alpha: Double, beta: Double)
      val alpha = getD("alpha", 0.0001)
      val beta = getD("beta", 0.0001)
      new AdaDelta(lr, alpha, beta)
    } else if (name.equalsIgnoreCase("AdaGrad")) {
      // AdaGrad(lr: Double, beta: Double)
      val beta = getD("beta", 0.0001)
      new AdaGrad(lr, beta)
    } else if (name.equalsIgnoreCase("Adam")) {
      // Adam(lr: Double, beta: Double, gamma: Double)
      val beta = getD("beta", 0.0001)
      val gamma = getD("gamma", 0.0001)
      new Adam(lr, beta, gamma)
    } else if (name.equalsIgnoreCase("FTRL")) {
      // FTRL(lr: Double, alpha: Double, beta: Double)
      val alpha = getD("alpha", 0.0001)
      val beta = getD("beta", 0.0001)
      new FTRL(lr, alpha, beta)
    } else if (name.equalsIgnoreCase("Momentum")) {
      // Momentum(lr: Double, momentum: Double)
      val momentum = getD("momentum", 0.0001)
      new Momentum(lr, momentum)
    } else if (name.equalsIgnoreCase("SGD")) {
      // SGD(lr: Double)
      new SGD(lr)
    } else { // Momentum(lr: Double, momentum: Double)
      val momentum = getD("momentum", 0.0001)
      new Momentum(lr, momentum)
    }

    opt.setRegL1Param(getD("regL1Param", 0.0))
    opt.setRegL2Param(getD("regL2Param", 0.0))

    opt
  }
}