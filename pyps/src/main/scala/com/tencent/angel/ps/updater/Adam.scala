package com.tencent.angel.ps.updater

import java.util.concurrent.Future

import com.tencent.angel.ml.psf.optimizer.AdamUpdateFunc
import com.tencent.angel.ps.variable.Variable
import org.apache.commons.logging.LogFactory

class Adam(override var lr: Double, val beta: Double, val gamma: Double) extends Optimizer {
  private val LOG = LogFactory.getLog(classOf[Adam])

  override val numSlot: Int = 3

  override def update[T](variable: Variable, epoch: Int, batchSize: Int = 1): Future[T] =  {
    val matClient = variable.getMatClient
    val matrixId = matClient.getMatrixId
    val numInnerRows = getNumInnerRows(variable)

    val func = new AdamUpdateFunc(matrixId, numInnerRows, gamma, epsilon, beta, lr, regL2Param, epoch, batchSize)
    matClient.update(func).asInstanceOf[Future[T]]
  }

  override def toString: String = {
    s"Adam gamma=$gamma beta=$beta lr=$lr regL2=$regL2Param epsilon=$epsilon"
  }

}
