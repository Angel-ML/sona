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

package org.apache.spark.angel.graph


import org.apache.spark.angel.graph.kcore.KCore
import org.apache.spark.angel.graph.louvain.Louvain
import org.apache.spark.angel.graph.utils.GraphIO
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext, SparkFunSuite}

import scala.util.Random


class AngelGraphSuite extends SparkFunSuite {
  private var spark: SparkSession = _
  private var sparkConf: SparkConf = _
  private var sc: SparkContext = _
  private val numPartition: Int = 2
  private val storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY
  private var numEdge: Long = -1
  private var maxNodeId: Long = -1

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession.builder()
      .master(s"local[$numPartition]")
      .appName("AngelGraph")
      .getOrCreate()

    sc = spark.sparkContext
    sparkConf = spark.sparkContext.getConf
  }

  override def afterAll(): Unit = {
    super.afterAll()
    spark.close()
  }

  def readData(input: String, sep: String = " "): DataFrame = {
    val rdd = sc.textFile(input).mapPartitions { iter =>
      val r = new Random()
      iter.map { line =>
        val arr = line.split(sep)
        val src = arr(0).toInt
        val dst = arr(1).toInt
        (r.nextInt, Row(src, dst))
      }
    }.repartition(numPartition).values.persist(storageLevel)

    numEdge = rdd.count()
    maxNodeId = rdd.map { case Row(src: Int, dst: Int) => math.max(src, dst) }.max().toLong + 1

    val schema = StructType(Array(StructField("src", IntegerType), StructField("dst", IntegerType)))
    val data = spark.createDataFrame(rdd, schema)

    println(s"numEdge=$numEdge maxNodeId=$maxNodeId")

    data
  }

  test("readData") {
    val input = "../data/angel/bc/edge"
    val data = readData(input)
    data.printSchema()
    data.show(10)
  }

  test("kcore") {
    val input = "../data/angel/bc/edge"
    val data = GraphIO.load(input, isWeighted = false, 0, 1, sep = " ")
    data.printSchema()
    data.show(10)

    val kCore = new KCore()
      .setPartitionNum(100)
      .setStorageLevel(storageLevel)
      .setPSPartitionNum(10)
      .setSrcNodeIdCol("src")
      .setDstNodeIdCol("dst")

    val mapping = kCore.transform(data)
    GraphIO.save(mapping, "trained_models/kCoreAlgo")
  }


  test("louvain") {
    val input = "../data/angel/bc/edge"
    val data = GraphIO.load(input, isWeighted = false, 0, 1, sep = " ")
    data.printSchema()
    data.show(10)
    sc.setCheckpointDir("trained_models/louvainAlgo/cp")

    val louvain = new Louvain()
      .setPartitionNum(4)
      .setStorageLevel(storageLevel)
      .setNumFold(2)
      .setNumOpt(5)
      .setBatchSize(100)
      .setDebugMode(true)
      .setEps(0.0)
      .setBufferSize(100000)
      .setIsWeighted(false)
      .setPSPartitionNum(2)
      .setSrcNodeIdCol("src")
      .setDstNodeIdCol("dst")

    val mapping = louvain.transform(data)
    GraphIO.save(mapping, "trained_models/louvainAlgo")
  }

//  test("word2vec") {
//    val input = "./data/angel/text8/text8.split.head"
//    val data = sc.textFile(input)
//    data.cache()
//
//    val (corpus) = corpusStringToIntWithoutRemapping(sc.textFile(input))
//    val docs = corpus.repartition(1)
////    val schema = StructType(Array(StructField("src", ArrayType[Int])))
////    val df = spark.createDataFrame(docs.map{case arr:Array[Int] => Row(arr)}, schema)
//
//    docs.cache()
//    docs.count()
//    data.unpersist()
//
//    val numDocs = docs.count()
//    val maxWordId = docs.map(_.max).max() + 1
//    val numTokens = docs.map(_.length).sum()
//    val maxLength = docs.map(_.length).max()
//
//    println(s"numDocs=$numDocs maxWordId=$maxWordId numTokens=$numTokens")
//
//    val word2vec = new Word2Vec()
//      .setEmbeddingDim(10)
//      .setBatchSize(100)
//      .setModel("cbow")
//      .setNegSample(5)
//      .setMaxIndex(maxWordId)
//      .setMaxLength(maxLength)
//      .setCheckpointInterval(1000)
//      .setNumEpoch(5)
//      .setNumPSPart(1)
//      .setPartitionNum(5)
//      .setStepSize(1.0)
//      .setWindowSize(5)
////    val model = word2vec.fit(df)
//  }

  def corpusStringToIntWithoutRemapping(data: RDD[String]): RDD[Array[Int]] = {
    data.filter(f => f != null && f.length > 0)
      .map(f => f.stripLineEnd.split("[\\s+|,]").map(s => s.toInt))
  }
}
