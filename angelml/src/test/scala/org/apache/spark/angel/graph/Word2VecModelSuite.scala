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

import org.apache.spark.{SparkConf, SparkContext, SparkFunSuite}
import org.apache.spark.angel.graph.embedding.Param
import org.apache.spark.angel.graph.embedding.word2vec.Word2VecModel
import org.apache.spark.angel.graph.utils.{Features, SubSampling}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

class Word2VecModelSuite extends SparkFunSuite {
  private var spark: SparkSession = _
  private var sparkConf: SparkConf = _
  private var sc: SparkContext = _

  val input = "./data/angel/text8/text8.split.head"
  val output = "model/"
  val numPartition = 1
  val lr = 0.1f
  val dim = 32
  val batchSize = 128
  val numPSPart = 2
  val numEpoch = 2
  val negative = 5
  val window = 3
  var param: Param = _
  var docs: RDD[Array[Int]] = _


  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession.builder()
      .master(s"local[$numPartition]")
      .appName("AngelGraph")
      .getOrCreate()

    sc = spark.sparkContext
    sparkConf = spark.sparkContext.getConf

    val data = sc.textFile(input)
    data.cache()
    val (corpus, _) = Features.corpusStringToInt(sc.textFile(input))
    //val subsampleTmp = SubSampling.sampling(corpus)
    //docs = subsampleTmp._2.repartition(2)
    //docs.cache()
    //docs.count()
    data.unpersist()
    docs = corpus
    val numDocs = docs.count()
    //val maxWordId = subsampleTmp._1
    val numTokens = docs.map(_.length).sum().toLong
    val maxLength = docs.map(_.length).max()
    val maxWordId = docs.map(f => f.max).max() + 1
    println(s"numDocs=$numDocs maxWordId=${maxWordId} numTokens=$numTokens maxLength=$maxLength")

    param = new Param()
    param.setLearningRate(lr)
    param.setEmbeddingDim(dim)
    param.setWindowSize(window)
    param.setBatchSize(batchSize)
    param.setSeed(Random.nextInt())
    param.setNumPSPart(Some(numPSPart))
    param.setNumEpoch(numEpoch)
    param.setNegSample(negative)
    param.setMaxIndex(maxWordId)
    param.setMaxLength(maxLength)
  }

  override def afterAll(): Unit = {
    super.afterAll()
    spark.close()
  }

  test("cbow") {
    param.setModel("cbow")
    val model = new Word2VecModel(param) {
      val messages = new ArrayBuffer[String]()

      // mock logs
      override def logTime(msg: String): Unit = {
        if (null != messages) {
          messages.append(msg)
        }
        println(msg)
      }
    }
    model.train(docs, param, "")
    model.save(output, 0)
    model.destroy()

    // extract loss
    val loss = model.messages.filter(_.contains("loss=")).map { line =>
      line.split(" ")(1).split("=").last.toFloat
    }

    for (i <- 0 until numEpoch - 1) {
      assert(loss(i + 1) < loss(i), s"loss increase: ${loss.mkString("->")}")
    }
  }

  test("skipgram") {
    param.setModel("skipgram")
    val model = new Word2VecModel(param) {
      val messages = new ArrayBuffer[String]()

      // mock logs
      override def logTime(msg: String): Unit = {
        if (null != messages) {
          messages.append(msg)
        }
        println(msg)
      }
    }
    model.train(docs, param, "")
    model.save(output, 0)
    model.destroy()

    // extract loss
    val loss = model.messages.filter(_.contains("loss=")).map { line =>
      line.split(" ")(1).split("=").last.toFloat
    }

    for (i <- 0 until numEpoch - 1) {
      assert(loss(i + 1) < loss(i), s"loss increase: ${loss.mkString("->")}")
    }
  }
}
