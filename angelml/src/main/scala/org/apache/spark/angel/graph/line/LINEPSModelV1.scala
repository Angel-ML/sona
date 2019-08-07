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

import com.tencent.angel.ml.math2.utils.RowType
import com.tencent.angel.ml.matrix.MatrixContext
import com.tencent.angel.ml.matrix.psf.get.base.GetFunc
import com.tencent.angel.ml.matrix.psf.update.base.UpdateFunc
import com.tencent.angel.sona.context.PSContext
import com.tencent.angel.sona.models.PSMatrix
import org.apache.spark.angel.psf.embedding.line.{Adjust, AdjustParam, Dot, DotParam}
import org.apache.spark.angel.graph.utils.NEModel
import org.apache.spark.angel.graph.utils.NEModel.NEDataSet
import org.apache.spark.rdd.RDD
import org.apache.spark.angel.graph.line.LINEPSModelV1.{LINEDataSet, buildDataBatches}
import org.apache.spark.angel.psf.embedding.NEDot.NEDotResult
import org.apache.spark.angel.psf.embedding.NESlice
import org.apache.spark.sql.SparkSession

import scala.util.Random

class LINEPSModelV1(embeddingMatName: String,
                    numNode: Int,
                    dimension: Int,
                    numPart: Int,
                    numEpoch: Int,
                    learningRate: Double,
                    negSample: Int,
                    batchSize: Int,
                    numNodesPerRow: Int = -1,
                    order: Int = 2,
                    seed: Int = Random.nextInt)
  extends NEModel(numNode, dimension, numPart, numNodesPerRow, order, true, seed) with Serializable {

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

  /**
    * Main function for training embedding model
    */
  def train(trainSet: RDD[(Int, Int)]): this.type = {
    psMatrix.psfUpdate(getInitFunc(trainSet.getNumPartitions, numNode, -1, negSample, -1))
    val trainBatches = buildDataBatches(trainSet, batchSize)

    for (epoch <- 1 to numEpoch) {
      // val alpha = learningRate * (1 - math.sqrt(epoch / numEpoch)).toFloat
      val data = trainBatches.next()
      val numPartitions = data.getNumPartitions
      val middle = data.mapPartitionsWithIndex((partitionId, iterator) =>
        sgdForPartition(partitionId, iterator, numPartitions, negSample, learningRate.toFloat),
        preservesPartitioning = true
      ).collect()
      val loss = middle.map(f => f._1).sum / middle.map(_._2).sum.toDouble
      val array = new Array[Long](6)
      middle.foreach(f => f._3.zipWithIndex.foreach(t => array(t._2) += t._1))
      NEModel.logTime(s"epoch=$epoch " +
        f"loss=$loss%2.4f " +
        s"dotTime=${array(0)} " +
        s"gradientTime=${array(1)} " +
        s"adjustTime=${array(2)} " +
        s"total=${middle.map(_._2).sum.toDouble} " +
        s"lossSum=${middle.map(_._1).sum}")
    }

    this
  }

  private def sgdForPartition(partitionId: Int,
                              iterator: Iterator[NEDataSet],
                              numPartitions: Int,
                              negative: Int,
                              alpha: Float): Iterator[(Double, Long, Array[Long])] = {

    def sgdForBatch(partitionId: Int,
                    seed: Int,
                    batch: NEDataSet,
                    batchId: Int): (Double, Long, Array[Long]) = {
      var (start, end) = (0L, 0L)
      // dot
      start = System.currentTimeMillis()
      val dots = psfGet(getDotFunc(batch, seed, negative, partitionId))
        .asInstanceOf[NEDotResult].result
      end = System.currentTimeMillis()
      val dotTime = end - start
      // gradient
      start = System.currentTimeMillis()
      val loss = doGrad(dots, negative, alpha)
      end = System.currentTimeMillis()
      val gradientTime = end - start
      // adjust
      start = System.currentTimeMillis()
      psfUpdate(getAdjustFunc(batch, seed, negative, dots, partitionId))
      end = System.currentTimeMillis()
      val adjustTime = end - start
      //       return loss
      if ((batchId + 1) % 100 == 0)
        NEModel.logTime(s"batchId=$batchId dotTime=$dotTime gradientTime=$gradientTime adjustTime=$adjustTime")
      (loss, dots.length.toLong, Array(dotTime, gradientTime, adjustTime))
    }

    PSContext.instance()

    iterator.zipWithIndex.map { case (batch, index) =>
      sgdForBatch(partitionId, rand.nextInt(), batch, index)
    }
  }

  def getMatrixContext: MatrixContext = {
    val sizeOccupiedPerNode = partDim * order
    require(numNodesPerRow <= Int.MaxValue / sizeOccupiedPerNode,
      s"size exceed Int.MaxValue, $numNodesPerRow * $sizeOccupiedPerNode > ${Int.MaxValue}")

    val rowCapacity = if (numNodesPerRow > 0) numNodesPerRow else Int.MaxValue / sizeOccupiedPerNode
    val numRow = (numNode - 1) / rowCapacity + 1

    val numNodesEachRow = if (numNodesPerRow > 0) numNodesPerRow else (numNode - 1) / numRow + 1
    val numCol = numPart.toLong * numNodesEachRow * sizeOccupiedPerNode
    val rowsInBlock = numRow
    val colsInBlock = numNodesEachRow * sizeOccupiedPerNode
    val validIndexNum = -1

    NEModel.logTime(s"matrix meta:\n" +
      s"colNum: $numCol\n" +
      s"rowNum: $numRow\n" +
      s"colsInBlock: $colsInBlock\n" +
      s"rowsInBlock: $rowsInBlock\n" +
      s"numNodesPerRow: $numNodesEachRow\n" +
      s"sizeOccupiedPerNode: $sizeOccupiedPerNode"
    )

    new MatrixContext(embeddingMatName, numRow, numCol, validIndexNum, rowsInBlock, colsInBlock, RowType.T_FLOAT_DENSE)
  }
}

object LINEPSModelV1 {

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
