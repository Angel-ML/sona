package com.tencent.angel.sona.common

import com.tencent.angel.ml.math2.utils.LabeledData
import org.apache.spark.broadcast.Broadcast
import com.tencent.angel.sona.core.ExecutorContext
import com.tencent.angel.sona.utils.DataUtils.Example
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.reflect.ClassTag

/**
  * Build manifold view for a RDD. A manifold RDD is to split a RDD to multiple RDD.
  * First, we shuffle the RDD and split it into several splits inside every partition.
  * Then, we hold the manifold RDD into cache.
  *
  * @param data     , RDD to be split
  * @param numSplit , the number of splits
  * @return
  */

import com.tencent.angel.sona.common.MathImplicits._

class ManifoldSplitter[T, U: ClassTag](iter: Iterator[T], numBatch: Int, trans: T => U)(implicit bcValue: Broadcast[ExecutorContext])
  extends Iterator[Array[U]] with Serializable {
  val numSamples: Int = bcValue.value.getSamplesInPartition
  val sizeBase: Int = numSamples / numBatch

  var index = 0

  override def hasNext: Boolean = {
    index < numBatch
  }

  override def next(): Array[U] = {
    val size = if (index < numSamples % numBatch) sizeBase + 1 else sizeBase
    val res = new Array[U](size)

    (0 until size).foreach { idx =>
      if (iter.hasNext) {
        res(idx) = trans(iter.next)
      }
    }

    index += 1
    res
  }
}


class ManifoldBuilder(data: RDD[Example],
                      numSplit: Int,
                      persist: StorageLevel = StorageLevel.MEMORY_AND_DISK,
                      preservesPartitioning: Boolean = true)(
  implicit dim: Long, bcValue: Broadcast[ExecutorContext]) extends Serializable {
  protected def trans(item: Example): LabeledData = {
    new LabeledData(item.features, item.label)
  }

  private def split(iterator: Iterator[Example]): Iterator[Array[LabeledData]] = {
    new ManifoldSplitter(iterator, numSplit, trans)
  }

  lazy val foldedRDD: RDD[Array[LabeledData]] = data.mapPartitions(
    itr => split(itr), preservesPartitioning).persist(persist)

  def manifoldRDD(): Seq[RDD[Array[LabeledData]]] = {
    Vector.tabulate(numSplit) { i =>
      foldedRDD.mapPartitions(itr => Iterator.single(itr.drop(i).next()), preservesPartitioning)
    }
  }
}
