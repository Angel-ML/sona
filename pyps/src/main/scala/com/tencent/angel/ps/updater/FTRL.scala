package com.tencent.angel.ps.updater

import java.util.concurrent.Future

import com.tencent.angel.ml.psf.optimizer.FTRLUpdateFunc
import com.tencent.angel.ps.variable.Variable
import org.apache.commons.logging.LogFactory

class FTRL(override var lr: Double, val alpha: Double, val beta: Double) extends Optimizer {
  private val LOG = LogFactory.getLog(classOf[FTRL])

  override val numSlot: Int = 3

  override def update[T](variable: Variable, epoch: Int, batchSize: Int = 1): Future[T] = {
    val matClient = variable.getMatClient
    val matrixId = matClient.getMatrixId
    val numInnerRows = getNumInnerRows(variable)

    val func = new FTRLUpdateFunc(matrixId, numInnerRows, alpha, beta, regL1Param, regL2Param, epoch, batchSize)

    matClient.update(func).asInstanceOf[Future[T]]
  }

  override def toString: String = {
    s"FTRL alpha=$alpha beta=$beta lr=$lr regL1=$regL1Param regL2=$regL2Param"
  }

}
