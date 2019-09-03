package com.tencent.angel.sona.ml.common

import com.tencent.angel.ml.math2.utils.LabeledData
import org.apache.spark.TaskContext
import MathImplicits._
import org.apache.spark.util.Example
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.reflect.ClassTag


/**
  * Build manifold view for a RDD. A manifold RDD is to split a RDD to multiple RDD.
  * First, we shuffle the RDD and split it into several splits inside every partition.
  * Then, we hold the manifold RDD into cache.
  */

class ManifoldSplitter[T, U: ClassTag](iter: Iterator[T], numBatch: Int, trans: T => U, partitionStat: Map[Int, Long])
  extends Iterator[Array[U]] with Serializable {
  val numSamples: Int = partitionStat(TaskContext.getPartitionId()).toInt
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
                      partitionStat: Map[Int, Long],
                      persist: StorageLevel = StorageLevel.MEMORY_AND_DISK,
                      preservesPartitioning: Boolean = true)(implicit dim: Long) extends Serializable {
  protected def trans(item: Example): LabeledData = {
    new LabeledData(item.features, item.label)
  }

  private def split(iterator: Iterator[Example]): Iterator[Array[LabeledData]] = {
    new ManifoldSplitter(iterator, numSplit, trans, partitionStat)
  }

  lazy val foldedRDD: RDD[Array[LabeledData]] = data.mapPartitions(
    itr => split(itr), preservesPartitioning)

  def manifoldRDD(): Seq[RDD[Array[LabeledData]]] = {
    Vector.tabulate(numSplit) { i =>
      foldedRDD.mapPartitions(itr => Iterator.single(itr.drop(i).next()), preservesPartitioning)
    }
  }
}
