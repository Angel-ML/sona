package com.tencent.angel.ps.variable


import java.util

import com.tencent.angel.apiserver.HandlerId
import com.tencent.angel.ml.psf.columns._
import com.tencent.angel.ml.servingmath2.VFactory
import com.tencent.angel.ml.servingmath2.matrix.Matrix
import com.tencent.angel.ml.servingmath2.vector._
import it.unimi.dsi.fastutil.ints.{IntArrays, IntOpenHashBigSet}
import it.unimi.dsi.fastutil.longs.{LongArrays, LongOpenHashBigSet}

@HandlerId(3)
class Embedding(name: String,
                numFeats: Long,
                embeddingSize: Int,
                dtype: String,
                updaterParams: Map[String, String],
                initializer: Initializer = new NormalInitializer(0.0, 1e-6))
  extends Variable(name, 2, Array[Long](embeddingSize, numFeats), dtype, -1, updaterParams, initializer) {

  private var embeddings: util.Map[java.lang.Long, Vector] = _
  private var uniqueIdx: Vector = _
  private var lastIndices: Vector = _

  override protected def doPull(epoch: Int, indices: Vector): Array[Vector] = {
    val matrixId = matClient.getMatrixId

    lastIndices = indices
    uniqueIdx = indices match {
      case v: IntIntVector if v.isDense =>
        val hashSet = new IntOpenHashBigSet(v.getStorage.getValues)
        val unique = hashSet.toIntArray
        IntArrays.quickSort(unique)
        VFactory.denseIntVector(unique)
      case v: IntDummyVector =>
        val hashSet = new IntOpenHashBigSet(v.getIndices)
        val unique = hashSet.toIntArray
        IntArrays.quickSort(unique)
        VFactory.denseIntVector(unique)
      case v: IntLongVector if v.isDense =>
        val hashSet = new LongOpenHashBigSet(v.getStorage.getValues)
        val unique = hashSet.toLongArray
        LongArrays.quickSort(unique)
        VFactory.denseLongVector(unique)
      case v: LongDummyVector =>
        val hashSet = new LongOpenHashBigSet(v.getIndices)
        val unique = hashSet.toLongArray
        LongArrays.quickSort(unique)
        VFactory.denseLongVector(unique)
    }

    val param = if (epoch == 0) {
      val initFunc = initializer.getInitFunc(matrixId, meta)
      new GetColsParam(matrixId, (0 until embeddingSize).toArray, uniqueIdx, initFunc)
    } else {
      new GetColsParam(matrixId, (0 until embeddingSize).toArray, uniqueIdx)
    }

    val func = new GetColsFunc(param)
    val result = matClient.get(func).asInstanceOf[GetColsResult]
    embeddings = result.results

    indices match {
      case v: IntIntVector if v.isDense =>
        v.getStorage.getValues.map { idx => embeddings.get(idx.toLong) }
      case v: IntDummyVector =>
        v.getIndices.map { idx => embeddings.get(idx.toLong) }
      case v: IntLongVector if v.isDense =>
        v.getStorage.getValues.map { idx => embeddings.get(idx) }
      case v: LongDummyVector =>
        v.getIndices.map { idx => embeddings.get(idx) }
    }
  }

  override protected def doPush(grad: Matrix, alpha: Double): Unit = {
    assert(embeddings != null)
    assert(uniqueIdx != null)
    assert(lastIndices != null)

    val matrixId = matClient.getMatrixId
    val start = embeddingSize * numSlot
    val end = embeddingSize * (numSlot + 1)

    val map = new util.HashMap[java.lang.Long, Vector](embeddings.size())
    lastIndices match {
      case v: IntIntVector if v.isDense =>
        v.getStorage.getValues.zipWithIndex { case (mapIdx: Int, rowIdx: Int) =>
          if (map.containsKey(mapIdx.toLong)) {
            map.get(mapIdx.toLong).iadd(grad.getRow(rowIdx).imul(alpha))
          } else {
            map.put(mapIdx.toLong, grad.getRow(rowIdx).imul(alpha))
          }
        }
      case v: IntDummyVector =>
        v.getIndices.zipWithIndex { case (mapIdx: Int, rowIdx: Int) =>
          if (map.containsKey(mapIdx.toLong)) {
            map.get(mapIdx.toLong).iadd(grad.getRow(rowIdx).imul(alpha))
          } else {
            map.put(mapIdx.toLong, grad.getRow(rowIdx).imul(alpha))
          }
        }
      case v: IntLongVector if v.isDense =>
        v.getStorage.getValues.zipWithIndex { case (mapIdx: Long, rowIdx: Int) =>
          if (map.containsKey(mapIdx)) {
            map.get(mapIdx).iadd(grad.getRow(rowIdx).imul(alpha))
          } else {
            map.put(mapIdx, grad.getRow(rowIdx).imul(alpha))
          }
        }
      case v: LongDummyVector =>
        v.getIndices.zipWithIndex { case (mapIdx: Long, rowIdx: Int) =>
          if (map.containsKey(mapIdx)) {
            map.get(mapIdx).iadd(grad.getRow(rowIdx).imul(alpha))
          } else {
            map.put(mapIdx, grad.getRow(rowIdx).imul(alpha))
          }
        }

    }

    val param = new UpdateColsParam(matrixId, (start until end).toArray, uniqueIdx, map)
    val func = new UpdateColsFunc(param)
    matClient.update(func).get()

    embeddings = null
    uniqueIdx = null
    lastIndices = null
  }
}
