package com.tencent.client.ps.updater

import java.util.concurrent.Future

import com.tencent.angel.matrix.psf.update.base.VoidResult
import com.tencent.client.ps.variable.Variable
import com.tencent.client.common.psf.optimizer.FTRLUpdateFunc
import org.apache.commons.logging.LogFactory

class FTRL(override var lr: Double, val alpha: Double, val beta: Double) extends Optimizer {
  private val LOG = LogFactory.getLog(classOf[FTRL])

  override val numSlot: Int = 3

  override def update(variable: Variable, epoch: Int, batchSize: Int = 1): Future[VoidResult] = {
    val matClient = variable.getMatClient
    val matrixId = matClient.getMatrixId
    val numInnerRows = getNumInnerRows(variable)

    val func = new FTRLUpdateFunc(matrixId, numInnerRows, alpha, beta, regL1Param, regL2Param, epoch, batchSize)

    matClient.update(func)
  }

  override def toString: String = {
    s"FTRL alpha=$alpha beta=$beta lr=$lr regL1=$regL1Param regL2=$regL2Param"
  }

}
