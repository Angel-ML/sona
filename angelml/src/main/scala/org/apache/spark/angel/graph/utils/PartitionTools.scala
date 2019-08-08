package org.apache.spark.angel.graph.utils

import com.tencent.angel.ml.matrix.{MatrixContext, PartContext}
import org.apache.spark.Partitioner
import org.apache.spark.graphx.PartitionStrategy

import scala.collection.JavaConversions._

object PartitionTools {

  def addPartition(ctx: MatrixContext, bounds: Array[Long]): Unit = {
    var lower = Long.MinValue
    var numPart = 0
    for (upper <- bounds ++ Array(Long.MaxValue)) {
      if (lower < upper) {
        ctx.addPart(new PartContext(0, 1, lower, upper, -1))
        lower = upper
        numPart += 1
      }
    }
    println(s"add numPart=$numPart: ${ctx.getParts().map(_.toString).mkString("\n")}")
  }

  def rangePartitionerFromBounds(rangeBounds: Array[Long], ascending: Boolean = true): Partitioner = {

    new Partitioner {
      private val binarySearch: (Array[Long], Long) => Int = SparkPrivateClassProxy.makeBinarySearch[Long]

      override def numPartitions: Int = {
        rangeBounds.length + 1
      }

      override def getPartition(key: Any): Int = {
        val k = key.asInstanceOf[Long]
        var partition = 0
        if (rangeBounds.length <= 128) {
          // If we have less than 128 partitions naive search
          while (partition < rangeBounds.length && k > rangeBounds(partition)) {
            partition += 1
          }
        } else {
          // Determine which binary search method to use only once.
          partition = binarySearch(rangeBounds, k)
          // binarySearch either returns the match location or -[insertion point]-1
          if (partition < 0) {
            partition = -partition - 1
          }
          if (partition > rangeBounds.length) {
            partition = rangeBounds.length
          }
        }
        if (ascending) {
          partition
        } else {
          rangeBounds.length - partition
        }
      }
    }
  }

  def rangePartitioner(maxIdx: Int, numPartition: Int, ascending: Boolean = true): Partitioner = {

    new Partitioner {
      println(s"new range partitioner, max node index = $maxIdx, partition number = $numPartition")

      private val binarySearch: (Array[Int], Int) => Int = SparkPrivateClassProxy.makeBinarySearch[Int]

      override def numPartitions: Int = {
        numPartition
      }

      def lengthPerPart: Int = maxIdx / numPartition

      def lengthLeft: Int = maxIdx % numPartition

      lazy val splits: Array[Int] = {
        val lengths = Array.fill(numPartition)(lengthPerPart)
        (0 until lengthLeft).foreach { idx =>
          lengths(idx) + 1
        }
        lengths.scanLeft(0)(_ + _).tail
      }

      println(s"range partition splits: ${splits.mkString(",")}")

      override def getPartition(key: Any): Int = {
        val k = key.asInstanceOf[Int]
        var partition = 0
        if (numPartition <= 128) {
          while (partition < numPartition && k >= splits(partition)) {
            partition += 1
          }
        } else {
          // Determine which binary search method to use only once.
          partition = binarySearch(splits, k)
          // binarySearch either returns the match location or -[insertion point]-1
          if (partition < 0) {
            partition = -partition - 1
          }
          if (partition >= splits.length) {
            partition = splits.length - 1
          }
        }
        if (ascending) {
          partition
        } else {
          splits.length - partition
        }
      }
    }
  }

  def edge2DPartitioner(numPartition: Int): Partitioner = {

    new Partitioner {
      override def numPartitions: Int = numPartition

      override def getPartition(key: Any): Int = {
        // require(key.isInstanceOf[(Long, Long)], s"Using 2D partition, the RDD key should be a (Long, Long) tuple")
        key match {
          case (x1: Long, x2: Long) =>
            PartitionStrategy.EdgePartition2D.getPartition(x1, x2, numPartitions)
          case _ => throw new Exception(s"Using 2D partition, the RDD key should be a (Long, Long) tuple")
        }
      }
    }
  }

}
