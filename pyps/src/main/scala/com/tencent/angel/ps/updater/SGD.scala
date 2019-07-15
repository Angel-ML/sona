package com.tencent.angel.ps.updater

import java.util.concurrent.Future

import com.tencent.angel.ml.psf.optimizer.PGDUpdateFunc
import com.tencent.angel.ps.variable.Variable
import org.apache.commons.logging.LogFactory

class SGD(override var lr: Double) extends Optimizer {
  private val LOG = LogFactory.getLog(classOf[SGD])

  override val numSlot: Int = 0

  override def update[T](variable: Variable, epoch: Int, batchSize: Int = 1): Future[T] = {
    val matClient = variable.getMatClient
    val matrixId = matClient.getMatrixId
    val numInnerRows = getNumInnerRows(variable)

    val func = new PGDUpdateFunc(matrixId, numInnerRows, lr, regL1Param, regL2Param, batchSize)

    matClient.update(func).asInstanceOf[Future[T]]
  }

  override def toString: String = {
    s"SGD lr=$lr regL2=$regL2Param regL1=$regL1Param"
  }

}
