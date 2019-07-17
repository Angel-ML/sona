package com.tencent.client.ps.variable

import java.util.concurrent.Future

trait Updater extends Serializable {
  val numSlot: Int

  def update[T](variable: Variable, epoch: Int, batchSize: Int): Future[T]
}