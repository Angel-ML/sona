package org.apache.spark.angel.ml.tree.util

import org.apache.spark.SparkContext
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD

object DataLoader {

  def parseLibsvm(line: String, dim: Int): (Double, Vector) = {
    val splits = line.split("\\s+|,").map(_.trim)
    val y = splits(0).toDouble

    val indices = new Array[Int](splits.length - 1)
    val values = new Array[Double](splits.length - 1)
    for (i <- 0 until splits.length - 1) {
      val kv = splits(i + 1).split(":")
      indices(i) = kv(0).toInt
      values(i) = kv(1).toDouble
    }

    (y, Vectors.sparse(dim, indices, values))
  }

  def loadLibsvm(input: String, dim: Int)
                (implicit sc: SparkContext): RDD[(Double, Vector)] = {
    sc.textFile(input)
      .map(_.trim)
      .filter(_.nonEmpty)
      .filter(!_.startsWith("#"))
      .map(line => parseLibsvm(line, dim))
  }
}
