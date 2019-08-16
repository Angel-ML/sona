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
package org.apache.spark.angel.ml.util

import java.util

import com.tencent.angel.client.AngelPSClient
import com.tencent.angel.ml.matrix.MatrixContext
import com.tencent.angel.ml.matrix.psf.aggr.enhance.ScalarAggrResult
import com.tencent.angel.mlcore.utils.MLException
import com.tencent.angel.ml.math2.storage.{IntDoubleSparseVectorStorage, LongDoubleSparseVectorStorage}
import com.tencent.angel.ml.math2.utils.RowType
import com.tencent.angel.ml.math2.vector.{IntDoubleVector, LongDoubleVector}
import com.tencent.angel.psagent.PSAgent
import com.tencent.angel.sona.core.ExecutorContext
import com.tencent.angel.sona.psf.StatsFunc
import it.unimi.dsi.fastutil.ints.Int2DoubleOpenHashMap
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap
import org.apache.spark.TaskContext
import org.apache.spark.angel.ml.linalg.{IntSparseVector, LongSparseVector}
import org.apache.spark.angel.ml.util.DataUtils.Example
import org.apache.spark.broadcast.Broadcast


class FeatureStats(val uid: String, val modelTypeString: String, val exeCtx: Broadcast[ExecutorContext]) extends Serializable {
  private val statsMatName = s"${uid}_stats"
  @transient private var numValidateFeatures: Long = -1

  private lazy val modelType: RowType = {
    modelTypeString match {
      case s if s.equalsIgnoreCase(RowType.T_DOUBLE_DENSE.toString) => RowType.T_DOUBLE_DENSE
      case s if s.equalsIgnoreCase(RowType.T_DOUBLE_SPARSE.toString) => RowType.T_DOUBLE_SPARSE
      case s if s.equalsIgnoreCase(RowType.T_DOUBLE_SPARSE_LONGKEY.toString) => RowType.T_DOUBLE_SPARSE_LONGKEY
      case _ => throw MLException("ModelType is not supported!")
    }
  }

  def createPSMat(psClient: AngelPSClient, numFeature: Long): Unit = {
    // name: String, rowNum: Int, colNum: Long, validIndexNum: Long, maxRowNumInBlock: Int, maxColNumInBlock: Long, rowType: RowType
    val matCtx: MatrixContext = new MatrixContext(statsMatName, 1, numFeature, -1,
      -1, -1, modelType)
    val list = new util.ArrayList[MatrixContext]()
    list.add(matCtx)
    psClient.createMatrices(list)
  }

  def getNumValidateFeatures(psAgent: PSAgent): Long = {
    if (numValidateFeatures == -1) {
      val statsMat = psAgent.getMatrixClient(statsMatName)
      val statsFunc = new StatsFunc(statsMat.getMatrixId)
      val getResult = statsMat.get(statsFunc)
      numValidateFeatures = getResult.asInstanceOf[ScalarAggrResult].getResult.toLong
    }

    numValidateFeatures
  }

  def partitionStatsWithPS(iter: Iterator[Example]): Iterator[util.HashMap[Int, Long]] = {
    val partitionId = TaskContext.getPartitionId()
    val partitionStat = new util.HashMap[Int, Long]()

    var count: Long = 0L
    val statsBatchSize = 10000
    var hashMap: AnyRef = null

    val headSample = if (iter.hasNext) {
      val Example(_, _, features) = iter.next()
      count += 1

      features match {
        case feat: IntSparseVector =>
          val openHashMap = new Int2DoubleOpenHashMap(statsBatchSize)
          feat.foreachActive { case (idx: Long, value: Double) =>
            openHashMap.put(idx.toInt, value)
          }
          hashMap = openHashMap
        case feat: LongSparseVector =>
          val openHashMap = new Long2DoubleOpenHashMap(statsBatchSize)
          feat.foreachActive { case (idx: Long, value: Double) =>
            openHashMap.put(idx, value)
          }
          hashMap = openHashMap
        case _ => throw MLException("Dense Vector is not supported!")
      }

      features
    } else {
      partitionStat.put(partitionId, 0L)
      return Iterator.single[util.HashMap[Int, Long]](partitionStat)
    }

    val psAgent = ExecutorContext.getPSAgent(exeCtx.value)
    val statsMat = psAgent.getMatrixClient(statsMatName)

    while (iter.hasNext) {
      val Example(_, _, features) = iter.next()
      features match {
        case feat: IntSparseVector =>
          val openHashMap = hashMap.asInstanceOf[Int2DoubleOpenHashMap]
          feat.foreachActive { case (idx: Long, value: Double) =>
            openHashMap.put(idx.toInt, value)
          }
        case feat: LongSparseVector =>
          val openHashMap = hashMap.asInstanceOf[Long2DoubleOpenHashMap]
          feat.foreachActive { case (idx: Long, value: Double) =>
            openHashMap.put(idx, value)
          }
        case _ => throw MLException("Dense Vector is not supported!")
      }
      count += 1

      if (count % statsBatchSize == 0 && count != 0) {
        features match {
          case feat: IntSparseVector =>
            val openHashMap = hashMap.asInstanceOf[Int2DoubleOpenHashMap]
            val storage = new IntDoubleSparseVectorStorage(feat.size.toInt, openHashMap)
            val vector = new IntDoubleVector(0, 0, 0, feat.size.toInt, storage)
            statsMat.increment(0, vector, true)
            openHashMap.clear()
          case feat: LongSparseVector =>
            val openHashMap = hashMap.asInstanceOf[Long2DoubleOpenHashMap]
            val storage = new LongDoubleSparseVectorStorage(feat.size, openHashMap)
            val vector = new LongDoubleVector(0, 0, 0, feat.size, storage)
            statsMat.increment(0, vector, true)
            openHashMap.clear()
          case _ => throw MLException("Dense Vector is not supported!")
        }
      }
    }

    headSample match {
      case feat: IntSparseVector =>
        val openHashMap = hashMap.asInstanceOf[Int2DoubleOpenHashMap]
        val storage = new IntDoubleSparseVectorStorage(feat.size.toInt, openHashMap)
        val vector = new IntDoubleVector(0, 0, 0, feat.size.toInt, storage)
        statsMat.increment(0, vector, true)
        openHashMap.clear()
      case feat: LongSparseVector =>
        val openHashMap = hashMap.asInstanceOf[Long2DoubleOpenHashMap]
        val storage = new LongDoubleSparseVectorStorage(feat.size, openHashMap)
        val vector = new LongDoubleVector(0, 0, 0, feat.size, storage)
        statsMat.increment(0, vector, true)
        openHashMap.clear()
      case _ => throw MLException("Dense Vector is not supported!")
    }

    partitionStat.put(partitionId, count)
    Iterator.single[util.HashMap[Int, Long]](partitionStat)
  }

  def partitionStats(iter: Iterator[Example]): Iterator[util.HashMap[Int, Long]] = {
    val partitionId = TaskContext.getPartitionId()
    val partitionStat = new util.HashMap[Int, Long]()

    partitionStat.put(partitionId, iter.length.toLong)
    Iterator.single[util.HashMap[Int, Long]](partitionStat)
  }

  def mergeMap(first: util.HashMap[Int, Long], second: util.HashMap[Int, Long]): util.HashMap[Int, Long] = {
    first.putAll(second)

    first
  }
}
