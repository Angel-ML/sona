package org.apache.spark.angelml.metaextract

import java.util

import com.tencent.angel.client.AngelPSClient
import com.tencent.angel.matrix.MatrixContext
import com.tencent.angel.matrix.psf.aggr.enhance.ScalarAggrResult
import com.tencent.angel.ml.core.conf.SharedConf
import com.tencent.angel.ml.core.utils.MLException
import com.tencent.angel.ml.math2.utils.RowType
import com.tencent.angel.ml.math2.vector.{IntDoubleVector, LongDoubleVector, Vector}
import com.tencent.angel.ml.math2.{MFactory, VFactory}
import com.tencent.angel.psagent.PSAgent
import com.tencent.angel.sona.core.{ExecutorContext, PSAgentContext, SparkEnvContext}
import com.tencent.angel.sona.psf.StatsFunc
import org.apache.spark.TaskContext
import org.apache.spark.angelml.linalg.{IntSparseVector, LongSparseVector}
import org.apache.spark.angelml.util.DataUtils.Example
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

  def createPSMat(psClient: AngelPSClient): Unit = {
    val matCtx: MatrixContext = new MatrixContext(statsMatName, 1, -1,
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

  def sparseStats(iter: Iterator[Example]): Iterator[util.HashMap[Int, Long]] = {
    val partitionId = TaskContext.getPartitionId()
    val partitionStat = new util.HashMap[Int, Long]()

    var count: Long = 0L
    val statsBatchSize = 10000
    val vecArray = new util.ArrayList[Vector](statsBatchSize)

    val headSample = if (iter.hasNext) {
      val Example(_, _, features) = iter.next()
      features match {
        case feat: IntSparseVector =>
          vecArray.add(VFactory.sortedDoubleVector(feat.size.toInt, feat.indices, feat.values))
        case feat: LongSparseVector =>
          vecArray.add(VFactory.sortedLongKeyDoubleVector(feat.size, feat.indices, feat.values))
        case _ => throw MLException("Dense Vector is not supported!")
      }
      count += 1

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
          vecArray.add(VFactory.sortedDoubleVector(feat.size.toInt, feat.indices, feat.values))
        case feat: LongSparseVector =>
          vecArray.add(VFactory.sortedLongKeyDoubleVector(feat.size, feat.indices, feat.values))
        case _ => throw MLException("Dense Vector is not supported!")
      }
      count += 1

      if (count % statsBatchSize == 0 && count != 0) {
        val batchMat = features match {
          case _: IntSparseVector =>
            val rows = (0 until vecArray.size()).toArray.map { idx => vecArray.get(idx).asInstanceOf[IntDoubleVector] }
            MFactory.rbIntDoubleMatrix(statsMat.getMatrixId, -1, rows)
          case _: LongSparseVector =>
            val rows = (0 until vecArray.size()).toArray.map { idx => vecArray.get(idx).asInstanceOf[LongDoubleVector] }
            MFactory.rbLongDoubleMatrix(statsMat.getMatrixId, -1, rows)
          case _ => throw MLException("Dense Vector is not supported!")
        }
        statsMat.increment(batchMat)
        vecArray.clear()
      }
    }

    val batchMat = headSample match {
      case _: IntSparseVector =>
        val rows = (0 until vecArray.size()).toArray.map { idx => vecArray.get(idx).asInstanceOf[IntDoubleVector] }
        MFactory.rbIntDoubleMatrix(statsMat.getMatrixId, -1, rows)
      case _: LongSparseVector =>
        val rows = (0 until vecArray.size()).toArray.map { idx => vecArray.get(idx).asInstanceOf[LongDoubleVector] }
        MFactory.rbLongDoubleMatrix(statsMat.getMatrixId, -1, rows)
      case _ => throw MLException("Dense Vector is not supported!")
    }
    statsMat.increment(batchMat)
    vecArray.clear()

    statsMat.clock(true)

    if (partitionId == 0) {
      psAgent.releaseMatrix(statsMatName)
    }

    partitionStat.put(partitionId, count)
    Iterator.single[util.HashMap[Int, Long]](partitionStat)
  }

  def denseStats(iter: Iterator[Example]): Iterator[util.HashMap[Int, Long]] = {
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
