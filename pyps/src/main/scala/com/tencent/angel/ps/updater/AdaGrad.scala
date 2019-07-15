package com.tencent.angel.ps.updater

import java.util.concurrent.Future

import com.tencent.angel.ml.psf.optimizer.AdaGradUpdateFunc
import com.tencent.angel.ps.variable.Variable
import org.apache.commons.logging.LogFactory

class AdaGrad(override var lr: Double, val beta: Double) extends Optimizer {
  private val LOG = LogFactory.getLog(classOf[AdaGrad])

  override val numSlot: Int = 2

  override def update[T](variable: Variable, epoch: Int, batchSize: Int = 1): Future[T] = {
    val matClient = variable.getMatClient
    val matrixId = matClient.getMatrixId
    val numInnerRows = getNumInnerRows(variable)

    val func = new AdaGradUpdateFunc(matrixId, numInnerRows, epsilon, beta, lr,
      regL1Param, regL2Param, epoch, batchSize)
    matClient.update(func).asInstanceOf[Future[T]]
  }

  override def toString: String = {
    s"AdaGrad beta=$beta lr=$lr regL2=$regL2Param regL1=$regL1Param epsilon=$epsilon"
  }

}