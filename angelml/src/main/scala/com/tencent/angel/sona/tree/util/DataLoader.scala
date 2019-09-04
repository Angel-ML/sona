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
package com.tencent.angel.sona.tree.util

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
