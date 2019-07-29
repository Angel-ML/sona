package com.tencent.client.worker.ps.updater

import java.util.concurrent.Future

import com.tencent.angel.matrix.psf.update.base.VoidResult
import com.tencent.client.common.psf.optimizer.AdaDeltaUpdateFunc
import com.tencent.client.worker.ps.variable.Variable
import org.apache.commons.logging.LogFactory

class AdaDelta(override var lr: Double, val alpha: Double, val beta: Double) extends Optimizer {
  private val LOG = LogFactory.getLog(classOf[AdaDelta])

  override val numSlot: Int = 3

  override def update(variable: Variable, epoch: Int, batchSize: Int = 1): Future[VoidResult] = {
    val matClient = variable.getMatClient
    val matrixId = matClient.getMatrixId
    val numInnerRows = getNumInnerRows(variable)

    val func = new AdaDeltaUpdateFunc(matrixId, numInnerRows, epsilon, alpha, beta, lr,
      regL1Param, regL2Param, epoch, batchSize)

    matClient.update(func)
  }

  override def toString: String = {
    s"AdaDelta alpha=$alpha beta=$beta lr=$lr regL2=$regL2Param regL1=$regL1Param epsilon=$epsilon"
  }
}