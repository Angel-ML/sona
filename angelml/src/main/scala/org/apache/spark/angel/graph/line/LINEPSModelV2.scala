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
import com.tencent.angel.sona.context.PSContext
import it.unimi.dsi.fastutil.ints.{Int2IntOpenHashMap, Int2ObjectOpenHashMap}
import org.apache.spark.angel.graph.line.LINEPSModelV1.{LINEDataSet, buildDataBatches}
import org.apache.spark.angel.graph.utils.NEModel
import org.apache.spark.angel.graph.utils.NEModel.NEDataSet
import org.apache.spark.angel.psf.embedding.NEModelRandomize.RandomizeUpdateParam
import org.apache.spark.rdd.RDD

import scala.util.Random


class LINEPSModelV2(embeddingMatName: String,
                    numNode: Int,
                    dimension: Int,
                    numPart: Int,
                    numEpoch: Int,
                    learningRate: Double,
                    negSample: Int,
                    batchSize: Int,
                    order: Int = 2,
                    seed: Int = Random.nextInt)
  extends NEModel(numNode, dimension, numPart, -1, order, true, seed) with Serializable {

  def getMatrixContext: MatrixContext = {
    // Create one ps matrix to hold the input vectors and the output vectors for all node
    val mc: MatrixContext = new MatrixContext(embeddingMatName, 1, numNode)
    mc.setMaxRowNumInBlock(1)
    mc.setMaxColNumInBlock(numNode / numPart)
    mc.setRowType(RowType.T_ANY_INTKEY_DENSE)
    mc.setValueType(classOf[LINENode])

    mc
  }

  def train(trainSet: RDD[(Int, Int)]): this.type = {
    // Get mini-batch data set
    val trainBatches = buildDataBatches(trainSet, batchSize)

    for (epoch <- 1 to numEpoch) {
      val data = trainBatches.next()
      val numPartitions = data.getNumPartitions
      val middle = data.mapPartitionsWithIndex((partitionId, iterator) =>
        sgdForPartition(partitionId, iterator, numPartitions, negSample, learningRate.toFloat),
        preservesPartitioning = true
      ).collect()
      val loss = middle.map(f => f._1).sum / middle.map(_._2).sum.toFloat
      val array = new Array[Long](6)
      middle.foreach(f => f._3.zipWithIndex.foreach(t => array(t._2) += t._1))

      NEModel.logTime(s"epoch=$epoch " +
        f"loss=$loss%2.4f " +
        s"sampleTime=${array(0)} getEmbeddingTime=${array(1)} " +
        s"dotTime=${array(2)} gradientTime=${array(3)} calUpdateTime=${array(4)} pushTime=${array(5)} " +
        s"total=${middle.map(_._2).sum.toFloat} lossSum=${middle.map(_._1).sum} ")
    }

    this
  }

  private def sgdForPartition(partitionId: Int,
                              iterator: Iterator[NEDataSet],
                              numPartitions: Int,
                              negative: Int,
                              alpha: Float): Iterator[(Float, Long, Array[Long])] = {

    PSContext.instance()
    iterator.zipWithIndex.map { case (batch, index) =>
      sgdForBatch(partitionId, batch.asInstanceOf[LINEDataSet], negative, index, alpha, rand.nextInt())
    }
  }

  private def sgdForBatch(partitionId: Int,
                          batch: LINEDataSet,
                          negative: Int,
                          batchId: Int,
                          alpha: Float, seed: Int): (Float, Long, Array[Long]) = {
    var start = 0L

    start = System.currentTimeMillis()
    val srcNodes = batch.src
    val destNodes = batch.dst
    val negativeSamples = negativeSample(srcNodes, negative, seed)
    val sampleTime = System.currentTimeMillis() - start

    // Get node embedding from PS
    start = System.currentTimeMillis()
    val getResult = getEmbedding(srcNodes, destNodes, negativeSamples, negative)
    val srcFeats: Int2ObjectOpenHashMap[Array[Float]] = getResult._1
    val targetFeats: Int2ObjectOpenHashMap[Array[Float]] = getResult._2

    val getEmbeddingTime = System.currentTimeMillis() - start

    // Get dot values
    start = System.currentTimeMillis()
    val dots = dot(srcNodes, destNodes, negativeSamples, srcFeats, targetFeats, negative)
    val dotTime = System.currentTimeMillis() - start

    // gradient
    start = System.currentTimeMillis()
    val loss = doGrad(dots, negative, alpha)
    val gradientTime = System.currentTimeMillis() - start

    start = System.currentTimeMillis()
    val (inputUpdates, outputUpdates) = adjust(srcNodes, destNodes, negativeSamples, srcFeats, targetFeats, negative, dots)
    val calUpdateTime = System.currentTimeMillis() - start

    start = System.currentTimeMillis()
    psMatrix.psfUpdate(new LINEAdjust(new LINEAdjustParam(matrixId, inputUpdates, outputUpdates, order)))
    val pushTime = System.currentTimeMillis() - start

    //if(batchId % 100 == 0) {
    NEModel.logTime(s"loss=$loss sampleTime=${sampleTime} getEmbeddingTime=${getEmbeddingTime} " +
      s"dotTime=${dotTime} gradientTime=${gradientTime} calUpdateTime=${calUpdateTime} " +
      s"pushTime=${pushTime}")
    //}

    (loss.toFloat, dots.length.toLong, Array(sampleTime, getEmbeddingTime, dotTime, gradientTime, calUpdateTime, pushTime))
  }

  private def getEmbedding(srcNodes: Array[Int], destNodes: Array[Int], negativeSamples: Array[Array[Int]], negative: Int) = {
    psMatrix.psfGet(new LINEGetEmbedding(new LINEGetEmbeddingParam(matrixId, srcNodes, destNodes,
      negativeSamples, order, negative))).asInstanceOf[LINEGetEmbeddingResult].getResult
  }

  private def dot(srcNodes: Array[Int], destNodes: Array[Int], negativeSamples: Array[Array[Int]],
                  srcFeats: Int2ObjectOpenHashMap[Array[Float]], targetFeats: Int2ObjectOpenHashMap[Array[Float]], negative: Int): Array[Float] = {
    val dots: Array[Float] = new Array[Float]((1 + negative) * srcNodes.length)
    if (order == 1) {
      var docIndex = 0
      for (i <- 0 until srcNodes.length) {
        val srcVec = srcFeats.get(srcNodes(i))
        // Get dot value for (src, dst)
        dots(docIndex) = arraysDot(srcVec, srcFeats.get(destNodes(i)))
        docIndex += 1

        // Get dot value for (src, negative sample)
        for (j <- 0 until negative) {
          dots(docIndex) = arraysDot(srcVec, srcFeats.get(negativeSamples(i)(j)))
          docIndex += 1
        }
      }
      dots
    } else {
      var docIndex = 0
      for (i <- 0 until srcNodes.length) {
        val srcVec = srcFeats.get(srcNodes(i))
        // Get dot value for (src, dst)
        dots(docIndex) = arraysDot(srcVec, targetFeats.get(destNodes(i)))
        docIndex += 1

        // Get dot value for (src, negative sample)
        for (j <- 0 until negative) {
          dots(docIndex) = arraysDot(srcVec, targetFeats.get(negativeSamples(i)(j)))
          docIndex += 1
        }
      }
      dots
    }
  }

  private def arraysDot(x: Array[Float], y: Array[Float]): Float = {
    var dotValue = 0.0f
    x.indices.foreach(i => dotValue += x(i) * y(i))
    dotValue
  }

  private def axpy(y: Array[Float], x: Array[Float], a: Float): Unit = {
    x.indices.foreach(i => y(i) += a * x(i))
  }

  private def div(x: Array[Float], f: Float): Unit = {
    x.indices.foreach(i => x(i) = x(i) / f)
  }

  private def adjust(srcNodes: Array[Int], destNodes: Array[Int], negativeSamples: Array[Array[Int]],
                     srcFeats: Int2ObjectOpenHashMap[Array[Float]], targetFeats: Int2ObjectOpenHashMap[Array[Float]],
                     negative: Int, dots: Array[Float]) = {
    if (order == 1) {
      val inputUpdateCounter = new Int2IntOpenHashMap(srcFeats.size())
      val inputUpdates = new Int2ObjectOpenHashMap[Array[Float]](srcFeats.size())

      var docIndex = 0
      srcNodes.indices.foreach { i =>
        // Src node grad
        val neule = new Array[Float](dimension)

        // Accumulate dst node embedding to neule
        val dstEmbedding = srcFeats.get(destNodes(i))
        var g = dots(docIndex)
        axpy(neule, dstEmbedding, g)

        // Use src node embedding to update dst node embedding
        val srcEmbedding = srcFeats.get(srcNodes(i))
        merge(inputUpdateCounter, inputUpdates, destNodes(i), g, srcEmbedding)

        docIndex += 1

        // Use src node embedding to update negative sample node embedding; Accumulate negative sample node embedding to neule
        for (j <- 0 until negative) {
          val negSampleEmbedding = srcFeats.get(negativeSamples(i)(j))
          g = dots(docIndex)

          // Accumulate negative sample node embedding to neule
          axpy(neule, negSampleEmbedding, g)

          // Use src node embedding to update negative sample node embedding
          merge(inputUpdateCounter, inputUpdates, negativeSamples(i)(j), g, srcEmbedding)
          docIndex += 1
        }

        // Use accumulation to update src node embedding, grad = 1
        merge(inputUpdateCounter, inputUpdates, srcNodes(i), 1, neule)
      }

      val iter = inputUpdateCounter.entrySet().iterator()
      while (iter.hasNext) {
        val entry = iter.next()
        div(inputUpdates.get(entry.getKey.toInt), entry.getValue.toFloat)
      }

      (inputUpdates, null)
    } else {
      val inputUpdateCounter = new Int2IntOpenHashMap(srcFeats.size())
      val inputUpdates = new Int2ObjectOpenHashMap[Array[Float]](srcFeats.size())

      val outputUpdateCounter = new Int2IntOpenHashMap(targetFeats.size())
      val outputUpdates = new Int2ObjectOpenHashMap[Array[Float]](targetFeats.size())

      var docIndex = 0
      srcNodes.indices.foreach { i =>
        // Src node grad
        val neule = new Array[Float](dimension)

        // Accumulate dst node embedding to neule
        val dstEmbedding = targetFeats.get(destNodes(i))
        var g = dots(docIndex)
        axpy(neule, dstEmbedding, g)

        // Use src node embedding to update dst node embedding
        val srcEmbedding = srcFeats.get(srcNodes(i))
        merge(outputUpdateCounter, outputUpdates, destNodes(i), g, srcEmbedding)

        docIndex += 1

        // Use src node embedding to update negative sample node embedding; Accumulate negative sample node embedding to neule
        for (j <- 0 until negative) {
          val negSampleEmbedding = targetFeats.get(negativeSamples(i)(j))
          g = dots(docIndex)

          // Accumulate negative sample node embedding to neule
          axpy(neule, negSampleEmbedding, g)

          // Use src node embedding to update negative sample node embedding
          merge(outputUpdateCounter, outputUpdates, negativeSamples(i)(j), g, srcEmbedding)
          docIndex += 1
        }

        // Use accumulation to update src node embedding, grad = 1
        merge(inputUpdateCounter, inputUpdates, srcNodes(i), 1, neule)
      }

      var iter = inputUpdateCounter.int2IntEntrySet().fastIterator()
      while (iter.hasNext) {
        val entry = iter.next()
        div(inputUpdates.get(entry.getIntKey), entry.getIntValue.toFloat)
      }

      iter = outputUpdateCounter.int2IntEntrySet().fastIterator()
      while (iter.hasNext) {
        val entry = iter.next()
        div(outputUpdates.get(entry.getIntKey), entry.getIntValue.toFloat)
      }

      (inputUpdates, outputUpdates)
    }
  }

  private def merge(inputUpdateCounter: Int2IntOpenHashMap, inputUpdates: Int2ObjectOpenHashMap[Array[Float]],
                    nodeId: Int, g: Float, update: Array[Float]): Int = {
    var grads: Array[Float] = inputUpdates.get(nodeId)
    if (grads == null) {
      grads = new Array[Float](dimension)
      inputUpdates.put(nodeId, grads)
      inputUpdateCounter.put(nodeId, 0)
    }

    //grads.iaxpy(update, g)
    axpy(grads, update, g)
    inputUpdateCounter.addTo(nodeId, 1)
  }

  private def negativeSample(nodeIds: Array[Int], sampleNum: Int, seed: Int): Array[Array[Int]] = {
    //val seed = UUID.randomUUID().hashCode()
    val rand = new Random(seed)
    val sampleNodes = new Array[Array[Int]](nodeIds.length)
    var nodeIndex: Int = 0

    for (nodeId <- nodeIds) {
      var sampleIndex: Int = 0
      sampleNodes(nodeIndex) = new Array[Int](sampleNum)
      while (sampleIndex < sampleNum) {
        val target = rand.nextInt(numNode)
        if (target != nodeId) {
          sampleNodes(nodeIndex)(sampleIndex) = target
          sampleIndex += 1
        }
      }
      nodeIndex += 1
    }
    sampleNodes
  }

  override def randomInitialize(seed: Int): Unit = {
    val beforeRandomize = System.currentTimeMillis()
    psMatrix.psfUpdate(new LINEModelRandomize(new RandomizeUpdateParam(matrixId, dimension / numPart, dimension, order, seed))).get()
    NEModel.logTime(s"Model successfully Randomized, cost ${(System.currentTimeMillis() - beforeRandomize) / 1000.0}s")
  }
}
