package com.tencent.client.worker.ps.variable

import java.util.concurrent.Future

import com.tencent.angel.matrix.psf.update.base.VoidResult

trait Updater extends Serializable {
  val numSlot: Int

  def update(variable: Variable, epoch: Int, batchSize: Int): Future[VoidResult]
}