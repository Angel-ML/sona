package com.tencent.client.ps.variable


import com.tencent.angel.matrix.psf.update.RandomNormal
import com.tencent.angel.matrix.psf.update.base.UpdateFunc
import com.tencent.angel.ps.server.data.request.{InitFunc, RandomNormalInitFunc}
import com.tencent.client.common.Meta
import com.tencent.client.ps.tensor.TensorMeta

abstract class Initializer {
  def getUpdateFunc(matId: Int, meta: Meta): UpdateFunc

  def getInitFunc(matId: Int, meta: Meta): InitFunc
}

class NormalInitializer(val mean: Double, val std: Double) extends Initializer {
  override def getInitFunc(matId: Int, meta: Meta): InitFunc = {
    new RandomNormalInitFunc(mean, std)
  }

  override def getUpdateFunc(matId: Int, meta: Meta): UpdateFunc = {
    val numRow = meta.getMatrixContext.getRowNum

    val originRows = meta match {
      case _: TensorMeta => numRow
      case vmeta: VariableMeta =>
        if (numRow == vmeta.numSlot + 1) {
          1
        } else {
          numRow / (vmeta.numSlot + 1)
        }
    }


    new RandomNormal(matId, 0, originRows, mean, std)
  }
}
