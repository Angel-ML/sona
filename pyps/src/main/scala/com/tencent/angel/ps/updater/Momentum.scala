package com.tencent.angel.ps.updater

import java.util.concurrent.Future

import com.tencent.angel.ml.psf.optimizer.MomentumUpdateFunc
import com.tencent.angel.ps.variable.Variable
import org.apache.commons.logging.LogFactory

class Momentum(override var lr: Double, val momentum: Double) extends Optimizer {
  private val LOG = LogFactory.getLog(classOf[Momentum])

  override val numSlot: Int = 2

  override def update[T](variable: Variable, epoch: Int, batchSize: Int = 1): Future[T] = {
    val matClient = variable.getMatClient
    val matrixId = matClient.getMatrixId
    val numInnerRows = getNumInnerRows(variable)

    val func = new MomentumUpdateFunc(matrixId, numInnerRows, momentum, lr, regL2Param, batchSize)

    matClient.update(func).asInstanceOf[Future[T]]
  }

  override def toString: String = {
    s"Momentum momentum=$momentum lr=$lr regL2=$regL2Param"
  }

}
