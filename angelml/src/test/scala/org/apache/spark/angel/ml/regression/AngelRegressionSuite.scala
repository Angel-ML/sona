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
package org.apache.spark.angel.ml.regression

import com.tencent.angel.sona.core.DriverContext
import org.apache.spark.sql.{DataFrameReader, SparkSession}
import org.apache.spark.{SparkConf, SparkFunSuite}


class AngelRegressionSuite extends SparkFunSuite {
  private var spark: SparkSession = _
  private var libsvm: DataFrameReader = _
  private var dummy: DataFrameReader = _
  private var sparkConf: SparkConf = _
  private var driverCtx: DriverContext = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession.builder()
      .master("local[2]")
      .appName("AngelRegression")
      .getOrCreate()

    libsvm = spark.read.format("libsvmex")
    dummy = spark.read.format("dummy")
    sparkConf = spark.sparkContext.getConf
    driverCtx = DriverContext.get(sparkConf)

    driverCtx.startAngelAndPSAgent()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    driverCtx.stopAngelAndPSAgent()
  }

  test("robustreg_train") {//todo???
//    val driverCtx = DriverContext.get(sparkConf)
//    driverCtx.startAngelAndPSAgent()
    val trainData = libsvm.load("./data/angel/abalone/abalone_8d_train.libsvm")

    val regressor = new AngelRegressor()
      .setModelJsonFile("./angelml/src/test/jsons/robustreg.json")
      .setModelSize(10)
      .setNumBatch(10)
      .setMaxIter(2)
      .setLearningRate(0.1)


    val model = regressor.fit(trainData)

    model.write.overwrite().save("trained_models/robustreg")

//    driverCtx.stopAngelAndPSAgent()
  }


  test("robust_predict") {
    val trainData = libsvm.load("./data/angel/abalone/abalone_8d_train.libsvm")

    val predictor = AngelRegressorModel.read.load("trained_models/robustreg")

    val res = predictor.transform(trainData)
    res.show()
    res.write.mode("overwrite").save("predict_results/robustreg")
  }

//  test("kmeans_train") {//todo???
//  //    val driverCtx = DriverContext.get(sparkConf)
//  //    driverCtx.startAngelAndPSAgent()
//  val trainData = libsvm.load("./data/angel/usps/usps_256d_train.libsvm")
//
//    val classifier = new AngelRegressor()
//      .setModelJsonFile("./angelml/src/test/jsons/kmeans.json")
//      .setNumBatch(10)
//      .setMaxIter(2)
//      .setLearningRate(0.1)
//
//    val model = classifier.fit(trainData)
//
//    model.write.overwrite().save("trained_models/kmeans")
//
//    //    driverCtx.stopAngelAndPSAgent()
//  }

  test("linreg_train") {//todo???

    val trainData = libsvm.load("./data/angel/abalone/abalone_8d_train.libsvm")

    val classifier = new AngelRegressor()
      .setModelJsonFile("./angelml/src/test/jsons/linreg.json")
      .setNumBatch(10)
      .setMaxIter(2)
      .setLearningRate(0.1)
      .setNumField(14)

    val model = classifier.fit(trainData)

    model.write.overwrite().save("trained_models/linreg")

  }

  test("linreg_predict") {
    val trainData = libsvm.load("./data/angel/abalone/abalone_8d_train.libsvm")

    val predictor = AngelRegressorModel.read.load("trained_models/robustreg")

    val res = predictor.transform(trainData)
    res.show()
    res.write.mode("overwrite").save("predict_results/robustreg")
  }
}
