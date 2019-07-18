package com.tencent.client.ps.updater

import java.util.concurrent.Future

import com.tencent.angel.matrix.psf.update.base.VoidResult
import com.tencent.client.ps.variable.Variable
import com.tencent.client.common.psf.optimizer.MomentumUpdateFunc
import org.apache.commons.logging.LogFactory

class Momentum(override var lr: Double, val momentum: Double) extends Optimizer {
  private val LOG = LogFactory.getLog(classOf[Momentum])

  override val numSlot: Int = 2

  override def update(variable: Variable, epoch: Int, batchSize: Int = 1): Future[VoidResult] = {
    val matClient = variable.getMatClient
    val matrixId = matClient.getMatrixId
    val numInnerRows = getNumInnerRows(variable)

    val func = new MomentumUpdateFunc(matrixId, numInnerRows, momentum, lr, regL2Param, batchSize)

    matClient.update(func)
  }

  override def toString: String = {
    s"Momentum momentum=$momentum lr=$lr regL2=$regL2Param"
  }

}
