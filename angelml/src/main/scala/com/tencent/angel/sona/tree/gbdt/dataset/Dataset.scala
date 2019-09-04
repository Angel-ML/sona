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
package com.tencent.angel.sona.tree.gbdt.dataset

import org.apache.spark.{Partitioner, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.{ArrayBuilder => AB}
import java.{util => ju}

import com.tencent.angel.sona.tree.gbdt.metadata.FeatureInfo
import com.tencent.angel.sona.tree.stats.quantile.HeapQuantileSketch
import com.tencent.angel.sona.tree.util.MathUtil

object Dataset extends Serializable {

  private val SPACE: Char = ' '
  private val COLON: Char = ':'

  def verticalPartition(path: String, fidToWorkerId: Array[Int],
                        fidToNewFid: Array[Int], numWorker: Int)
                       (implicit sc: SparkContext)
  : RDD[(Array[Float], Dataset[Int, Float])] = {
    val partitions = sc.textFile(path).repartition(numWorker)
      .mapPartitions(iterator => {
        // initialize placeholder
        val indices = Array.ofDim[AB.ofInt](numWorker)
        val values = Array.ofDim[AB.ofFloat](numWorker)
        val indexEnds = Array.ofDim[AB.ofInt](numWorker)
        val labels = new AB.ofFloat
        for (workerId <- 0 until numWorker) {
          indices(workerId) = new AB.ofInt
          values(workerId) = new AB.ofFloat
          indexEnds(workerId) = new AB.ofInt
        }
        val curIndexEnds = Array.ofDim[Int](numWorker)
        // iterate instances
        while (iterator.hasNext) {
          val line = iterator.next().trim
          if (!(line.isEmpty || line.startsWith("#"))) {
            val str = line.toCharArray
            var start = 0
            var end = 0
            // parse instance label
            while (end < str.length && str(end) != SPACE) end += 1
            val label = new String(str, start, end - start).trim.toFloat
            labels += label
            // parse instance (feature, value) pairs
            while (end < str.length) {
              start = end + 1
              while (str(end) != COLON) end += 1
              val fid = new String(str, start, end - start).trim.toInt
              start = end + 1
              while (end < str.length && str(end) != SPACE) end += 1
              val fvalue = new String(str, start, end - start).trim.toFloat
              val workerId = fidToWorkerId(fid)
              val newFid = fidToNewFid(fid)
              indices(workerId) += newFid
              values(workerId) += fvalue
              curIndexEnds(workerId) += 1
            }
            // set worker indexEnds
            for (workerId <- 0 until numWorker)
              indexEnds(workerId) += curIndexEnds(workerId)
          }
        }

        val partLabels = labels.result()
        val partId = TaskContext.getPartitionId()
        (0 until numWorker).map(workerId => {
          val partIndices = indices(workerId).result()
          val partValues = values(workerId).result()
          val partIndexEnds = indexEnds(workerId).result()
          val partition = new Partition[Int, Float](
            partIndices, partValues, partIndexEnds)
          // in case memory is limited
          indices(workerId) = null
          values(workerId) = null
          indexEnds(workerId) = null
          // result: (target workerId, (original workerId, labels, partition))
          if (workerId == partId)
            (workerId, (partId, partLabels, partition))
          else
            (workerId, (partId, null, partition))
        }).iterator
      })

    partitions.partitionBy(new IdenticalPartitioner(numWorker))
      .mapPartitions(iterator => {
        val (workerIds, workerParts) = iterator.toArray.unzip
        val workerId = workerIds.head
        require(workerIds.forall(_ == workerId))
        val oriPartId = workerParts.map(_._1)
        require(oriPartId.distinct.length == oriPartId.length)
        val sorted = workerParts.sortBy(_._1)
        val partLabels = sorted.map(_._2)
        require(partLabels.count(_ != null) == 1)
        Iterator((partLabels.find(_ != null).get, Dataset[Int, Float](
          workerId, sorted.map(_._3)
        )))
      })
  }

  def horizontalPartition(path: String, numWorker: Int,
                          numPartPerWorkerOpt: Option[Int] = None)
                         (implicit sc: SparkContext)
  : RDD[(Array[Float], Dataset[Int, Float])] = {
    val numPartPerWorker = numPartPerWorkerOpt.getOrElse(numWorker)
    sc.textFile(path).repartition(numWorker)
      .mapPartitions(iterator => {
        // initialize placeholder
        val indices = Array.ofDim[AB.ofInt](numPartPerWorker)
        val values = Array.ofDim[AB.ofFloat](numPartPerWorker)
        val indexEnds = Array.ofDim[AB.ofInt](numPartPerWorker)
        val labels = Array.ofDim[AB.ofFloat](numPartPerWorker)
        for (partId <- 0 until numPartPerWorker) {
          indices(partId) = new AB.ofInt
          values(partId) = new AB.ofFloat
          indexEnds(partId) = new AB.ofInt
          labels(partId) = new AB.ofFloat
        }
        val curIndexEnds = Array.ofDim[Int](numPartPerWorker)
        var numIns = 0
        var curPartId = 0
        // iterate instances
        while (iterator.hasNext) {
          val line = iterator.next().trim
          if (!(line.isEmpty || line.startsWith("#"))) {
            val str = line.toCharArray
            var start = 0
            var end = 0
            // parse instance label
            while (end < str.length && str(end) != SPACE) end += 1
            val label = new String(str, start, end - start).trim.toFloat
            labels(curPartId) += label
            // parse instance (feature, value) pairs
            while (end < str.length) {
              start = end + 1
              while (str(end) != COLON) end += 1
              val fid = new String(str, start, end - start).trim.toInt
              start = end + 1
              while (end < str.length && str(end) != SPACE) end += 1
              val fvalue = new String(str, start, end - start).trim.toFloat
              indices(curPartId) += fid
              values(curPartId) += fvalue
              curIndexEnds(curPartId) += 1
            }
            // set part indexEnds
            indexEnds(curPartId) += curIndexEnds(curPartId)
            // put instance to partitions in round-robin manner
            numIns += 1
            curPartId += 1
            if (curPartId == numPartPerWorker)
              curPartId = 0
          }
        }

        val workerLabels = Array.ofDim[Float](numIns)
        val workerParts = Array.ofDim[Partition[Int, Float]](numPartPerWorker)
        var offset = 0
        for (i <- 0 until numPartPerWorker) {
          val partLabels = labels(i).result(); labels(i) = null
          Array.copy(partLabels, 0, workerLabels, offset, partLabels.length)
          offset += partLabels.length
          val partIndices = indices(i).result(); indices(i) = null
          val partValues = values(i).result(); values(i) = null
          val partIndexEnds = indexEnds(i).result(); indexEnds(i) = null
          workerParts(i) = Partition[Int, Float](partIndices, partValues, partIndexEnds)
        }

        val workerId = TaskContext.getPartitionId()
        Iterator((workerLabels, Dataset[Int, Float](workerId, workerParts)))
      })
  }

  def createSketches(dataset: Dataset[Int, Float],
                     maxDim: Int): Array[HeapQuantileSketch] = {
    val sketches = Array.ofDim[HeapQuantileSketch](maxDim)
    for (fid <- 0 until maxDim)
      sketches(fid) = new HeapQuantileSketch()
    dataset.partitions.foreach(partition => {
      val numKV = partition.numKVPair
      val indices = partition.indices
      val values = partition.values
      for (i <- 0 until numKV)
        sketches(indices(i)).update(values(i))
    })
    sketches
  }

  def binning(dataset: Dataset[Int, Float],
              featureInfo: FeatureInfo): Dataset[Int, Int] = {
    val res = Dataset[Int, Int](dataset.id,
      dataset.numPartition, dataset.numInstance)
    for (partId <- 0 until dataset.numPartition) {
      val indices = dataset.partitions(partId).indices
      val values = dataset.partitions(partId).values
      val bins = Array.ofDim[Int](indices.length)
      for (i <- indices.indices)
        bins(i) = MathUtil.indexOf(featureInfo.getSplits(indices(i)), values(i))
      val indexEnds = dataset.partitions(partId).indexEnds
      res.appendPartition(indices, bins, indexEnds)
    }
    res
  }

  def apply[@specialized(Byte, Short, Int, Long, Float, Double) K,
  @specialized(Byte, Short, Int, Long, Float, Double) V]
  (id: Int, maxNumPartition: Int, maxNumInstance: Int): Dataset[K, V] =
    new Dataset[K, V](id, maxNumPartition, maxNumInstance)

  def apply[@specialized(Byte, Short, Int, Long, Float, Double) K,
  @specialized(Byte, Short, Int, Long, Float, Double) V]
  (id: Int, partitions: Seq[Partition[K, V]]): Dataset[K, V] = {
    val numPartition = partitions.length
    val numInstance = partitions.map(_.size).sum
    val res = new Dataset[K, V](id, numPartition, numInstance)
    partitions.foreach(res.appendPartition)
    res
  }
}

private[tree] case class Partition
[@specialized(Byte, Short, Int, Long, Float, Double) K,
@specialized(Byte, Short, Int, Long, Float, Double) V]
(indices: Array[K], values: Array[V], indexEnds: Array[Int]) {
  {
    assert(indices.length == values.length,
      s"Mismatch size of indices and values: ${indices.length} vs. ${values.length}")
    for (i <- 1 until size)
      assert(indexEnds(i - 1) <= indexEnds(i), s"IndexEnds should be non-decreasing")
  }

  def size: Int = indexEnds.length

  def numKVPair: Int = indices.length
}

class Dataset
[@specialized(Byte, Short, Int, Long, Float, Double) K,
@specialized(Byte, Short, Int, Long, Float, Double) V]
(val id: Int, maxNumPartition: Int, maxNumInstance: Int) extends Serializable {
  @transient private[tree] val partitions = Array.ofDim[Partition[K, V]](maxNumPartition)
  @transient private[tree] val partOffsets = Array.ofDim[Int](maxNumPartition)
  @transient private[tree] val insLayouts = Array.ofDim[Int](maxNumInstance)
  private[tree] var numPartition = 0
  private[tree] var numInstance = 0

  def get(insId: Int, fid: Int): V = {
    val partId = insLayouts(insId)
    val partition = partitions(partId)
    val partInsId = insId - partOffsets(partId)
    val start = if (partInsId == 0) 0 else partition.indexEnds(partInsId - 1)
    val end = partition.indexEnds(partInsId)
    val indices = partition.indices.asInstanceOf[Array[Int]]
    val t = ju.Arrays.binarySearch(indices, start, end, fid)
    if (t >= 0) partition.values(t) else (-1).asInstanceOf[V]
  }

  def appendPartition(partition: Partition[K, V]): Unit = {
    require(numPartition < maxNumPartition && numInstance + partition.size <= maxNumInstance)
    val partId = numPartition
    partitions(partId) = partition
    if (partId == 0) {
      partOffsets(partId) = 0
    } else {
      partOffsets(partId) = partOffsets(partId - 1) + partitions(partId - 1).size
    }
    for (i <- 0 until partition.size)
      insLayouts(partOffsets(partId) + i) = partId
    numPartition += 1
    numInstance += partition.size
  }

  def appendPartition(indices: Array[K], values: Array[V], indexEnds: Array[Int]): Unit =
    appendPartition(new Partition[K, V](indices, values, indexEnds))

  def size: Int = numInstance

  def numKVPair: Int = partitions.map(_.numKVPair).sum
}

// IdenticalPartitioner for shuffle operation
class IdenticalPartitioner(numWorkers: Int) extends Partitioner {
  override def numPartitions: Int = numWorkers

  override def getPartition(key: Any): Int = {
    val partId = key.asInstanceOf[Int]
    require(partId < numWorkers, s"Partition id $partId exceeds maximum partition $numWorkers")
    partId
  }
}
