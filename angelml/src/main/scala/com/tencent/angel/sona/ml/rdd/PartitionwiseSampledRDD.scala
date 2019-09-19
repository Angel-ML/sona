package com.tencent.angel.sona.ml.rdd

import java.util.Random

import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.random.RandomSampler

import scala.reflect.ClassTag

private[sona] class PartitionwiseSampledRDDPartition(val prev: Partition, val seed: Long)
  extends Partition with Serializable {
  override val index: Int = prev.index
}

/**
  * An RDD sampled from its parent RDD partition-wise. For each partition of the parent RDD,
  * a user-specified [[org.apache.spark.util.random.RandomSampler]] instance is used to obtain
  * a random sample of the records in the partition. The random seeds assigned to the samplers
  * are guaranteed to have different values.
  *
  * @param prev                  RDD to be sampled
  * @param sampler               a random sampler
  * @param preservesPartitioning whether the sampler preserves the partitioner of the parent RDD
  * @param seed                  random seed
  * @tparam T input RDD item type
  * @tparam U sampled RDD item type
  */
private[sona] class PartitionwiseSampledRDD[T: ClassTag, U: ClassTag](
                                                                       prev: RDD[T],
                                                                       sampler: RandomSampler[T, U],
                                                                       preservesPartitioning: Boolean,
                                                                       @transient private val seed: Long = (new Random).nextLong)
  extends RDD[U](prev) {

  @transient override val partitioner = if (preservesPartitioning) prev.partitioner else None

  override def getPartitions: Array[Partition] = {
    val random = new Random(seed)
    firstParent[T].partitions.map(x => new PartitionwiseSampledRDDPartition(x, random.nextLong()))
  }

  override def getPreferredLocations(split: Partition): Seq[String] =
    firstParent[T].preferredLocations(split.asInstanceOf[PartitionwiseSampledRDDPartition].prev)

  override def compute(splitIn: Partition, context: TaskContext): Iterator[U] = {
    val split = splitIn.asInstanceOf[PartitionwiseSampledRDDPartition]
    val thisSampler = sampler.clone
    thisSampler.setSeed(split.seed)
    thisSampler.sample(firstParent[T].iterator(split.prev, context))
  }
}
