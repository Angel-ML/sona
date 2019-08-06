/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017-2018 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */


package org.apache.spark.angel.graph.line

import com.tencent.angel.ml.matrix.psf.get.base.GetFunc
import com.tencent.angel.ml.matrix.psf.update.base.UpdateFunc
import com.tencent.angel.spark.ml.psf.embedding.line.{Adjust, AdjustParam, Dot, DotParam}
import org.apache.spark.angel.graph.line.LINEPSModel.LINEDataSet
import org.apache.spark.angel.graph.utils.NEModel
import org.apache.spark.angel.graph.utils.NEModel.NEDataSet
import org.apache.spark.rdd.RDD

import scala.util.Random

class LINEPSModel(numNode: Int,
                  dimension: Int,
                  numPart: Int,
                  numEpoch: Int,
                  learningRate: Double,
                  checkpointInterval: Int,
                  negSample: Int,
                  batchSize: Int,
                  numNodesPerRow: Int = -1,
                  order: Int = 2,
                  seed: Int = Random.nextInt)
  extends NEModel(numNode, dimension, numPart, numNodesPerRow, order, true, seed) {

  def train(trainSet: RDD[(Int, Int)], path: String): this.type = {
    psMatrix.psfUpdate(getInitFunc(trainSet.getNumPartitions, numNode, -1, negSample, -1))
    val iterator = LINEPSModel.buildDataBatches(trainSet, batchSize)
    train(iterator, negSample, numEpoch, learningRate.toFloat, checkpointInterval, path)

    this
  }

  override def getDotFunc(data: NEDataSet, batchSeed: Int, ns: Int, partitionId: Int): GetFunc = {
    val lineData = data.asInstanceOf[LINEDataSet]
    val param = new DotParam(matrixId, batchSeed, partitionId, lineData.src, lineData.dst)
    new Dot(param)
  }

  override def getAdjustFunc(data: NEDataSet,
                             batchSeed: Int,
                             ns: Int,
                             grad: Array[Float],
                             partitionId: Int): UpdateFunc = {
    val lineData = data.asInstanceOf[LINEDataSet]
    val param = new AdjustParam(matrixId, batchSeed, ns, partitionId, grad, lineData.src, lineData.dst)
    new Adjust(param)
  }
}

object LINEPSModel {

  def buildDataBatches(trainSet: RDD[(Int, Int)], batchSize: Int): Iterator[RDD[NEDataSet]] = {
    new Iterator[RDD[NEDataSet]] with Serializable {
      override def hasNext(): Boolean = true

      override def next(): RDD[NEDataSet] = {
        trainSet.mapPartitions { iter =>
          val shuffledIter = Random.shuffle(iter)
          asLineBatch(shuffledIter, batchSize)
        }
      }
    }
  }

  private def asLineBatch(iter: Iterator[(Int, Int)], batchSize: Int): Iterator[NEDataSet] = {
    val src = new Array[Int](batchSize)
    val dst = new Array[Int](batchSize)
    new Iterator[NEDataSet] {
      override def hasNext: Boolean = iter.hasNext

      override def next(): NEDataSet = {
        var pos = 0
        while (iter.hasNext && pos < batchSize) {
          val (s, d) = iter.next()
          src(pos) = s
          dst(pos) = d
          pos += 1
        }
        if (pos < batchSize) LINEDataSet(src.take(pos), dst.take(pos)) else LINEDataSet(src, dst)
      }
    }
  }

  case class LINEDataSet(src: Array[Int], dst: Array[Int]) extends NEDataSet

}
