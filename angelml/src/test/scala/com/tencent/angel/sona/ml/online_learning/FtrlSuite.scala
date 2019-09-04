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
package com.tencent.angel.sona.ml.online_learning

import com.tencent.angel.ml.math2.utils.RowType
import com.tencent.angel.sona.context.PSContext
import com.tencent.angel.sona.core.DriverContext
import com.tencent.angel.sona.graph.utils.DataLoader
import com.tencent.angel.sona.online_learning.FTRL
import org.apache.hadoop.fs.Path
import org.apache.spark.angel.ml.metric.AUC
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.SparkUtil
import org.apache.spark.{SparkConf, SparkContext, SparkFunSuite}


class FtrlSuite extends SparkFunSuite {
  private var spark: SparkSession = _
  private var sparkConf: SparkConf = _
  private var sc: SparkContext = _
  private var driverCtx: DriverContext = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession.builder()
      .master(s"local[2]")
      .appName("Angel online learning")
      .getOrCreate()

    sc = spark.sparkContext
    sparkConf = spark.sparkContext.getConf
    PSContext.getOrCreate(sc)
  }

  override def afterAll(): Unit = {
    super.afterAll()
    PSContext.stop()
    spark.close()
  }

  def train(params: Map[String, String]): Unit = {

    val alpha = params.getOrElse("alpha", "2.0").toDouble
    val beta = params.getOrElse("beta", "1.0").toDouble
    val lambda1 = params.getOrElse("lambda1", "0.1").toDouble
    val lambda2 = params.getOrElse("lambda2", "100.0").toDouble
    val dim = params.getOrElse("dim", "149").toLong
    val input = params.getOrElse("input", "data/angel/census/census_148d_train.libsvm")
    val dataType = params.getOrElse("dataType", "libsvm")
    val batchSize = params.getOrElse("batchSize", "100").toInt
    val partNum = params.getOrElse("partNum", "10").toInt
    val numEpoch = params.getOrElse("numEpoch", "3").toInt
    val output = params.getOrElse("output", "file:///model")
    val loadPath = params.getOrElse("load", "")

    val opt = new FTRL(lambda1, lambda2, alpha, beta)
    opt.init(dim, RowType.T_FLOAT_SPARSE_LONGKEY)

    val sc = SparkContext.getOrCreate()
    val inputData = sc.textFile(input)
    val data = dataType match {
      case "libsvm" =>
        inputData .map(s => (DataLoader.parseLongFloat(s, dim), DataLoader.parseLabel(s, false)))
          .map {
            f =>
              f._1.setY(f._2)
              f._1
          }
      case "dummy" =>
        inputData .map(s => (DataLoader.parseLongDummy(s, dim), DataLoader.parseLabel(s, false)))
          .map {
            f =>
              f._1.setY(f._2)
              f._1
          }
    }
    val size = data.count()

    if (loadPath.size > 0)
      opt.load(loadPath + "/back")

    for (epoch <- 1 until numEpoch) {
      val totalLoss = data.mapPartitions {
        case iterator =>
          val loss = iterator
            .sliding(batchSize, batchSize)
            .map(f => opt.optimize(f.toArray)).sum
          Iterator.single(loss)
      }.sum()

      val scores = data.mapPartitions {
        case iterator =>
          opt.predict(iterator.toArray).iterator}
      val auc = new AUC().calculate(scores)

      println(s"epoch=$epoch loss=${totalLoss / size} auc=$auc")
    }

    if (output.length > 0) {
      val weight = opt.weight
      opt.save(output + "/back")
      opt.saveWeight(output + "/weight")
    }
  }

  def predict(params: Map[String, String]): Unit = {

    val dim = params.getOrElse("dim", "149").toLong
    val input = params.getOrElse("input", "data/angel/census/census_148d_train.libsvm")
    val dataType = params.getOrElse("dataType", "libsvm")
    val partNum = params.getOrElse("partNum", "10").toInt
    val isTraining = params.getOrElse("isTraining", "false").toBoolean
    val hasLabel = params.getOrElse("hasLabel", "true").toBoolean
    val loadPath = params.getOrElse("load", "file:///model")
    val predictPath = params.getOrElse("predict", "file:///model/predict")


    val opt = new FTRL()
    opt.init(dim, RowType.T_FLOAT_SPARSE_LONGKEY)

    val sc = SparkContext.getOrCreate()

    val inputData = sc.textFile(input)
    val data = dataType match {
      case "libsvm" =>
        inputData .map(s =>
          (DataLoader.parseLongFloat(s, dim, isTraining, hasLabel)))
      case "dummy" =>
        inputData .map(s =>
          (DataLoader.parseLongDummy(s, dim, isTraining, hasLabel)))
    }

    if (loadPath.size > 0) {
      opt.load(loadPath + "/weight")
    }

    val scores = data.mapPartitions {
      case iterator =>
        opt.predict(iterator.toArray, false).iterator
    }

    val path = new Path(predictPath)
    val fs = path.getFileSystem(sc.hadoopConfiguration)
    if (fs.exists(path)) {
      fs.delete(path, true)
    }

    scores.saveAsTextFile(predictPath)
  }

  test("ftrl") {
    val params = SparkUtil.parse(Array(""))
    val actionType = params.getOrElse("actionType", "train").toString
    if (actionType == "train" || actionType == "incTrain") {
      train(params)
    } else {
      predict(params)
    }
  }

}
