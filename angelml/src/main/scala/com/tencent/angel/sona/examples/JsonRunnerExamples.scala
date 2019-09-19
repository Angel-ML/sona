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
package com.tencent.angel.sona.examples

import com.tencent.angel.sona.core.DriverContext
import com.tencent.angel.sona.ml.classification.{AngelClassifier, AngelClassifierModel}
import com.tencent.angel.sona.ml.regression.{AngelRegressor, AngelRegressorModel}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable

object JsonRunnerExamples {

  def parse(args: Array[String]): Map[String, String] = {
    val cmdArgs = new mutable.HashMap[String, String]()
    println("parsing parameter")
    for (arg <- args) {
      val KEY_VALUE_SEP = ":"
      val sepIdx = arg.indexOf(KEY_VALUE_SEP)
      if (sepIdx != -1) {
        val k = arg.substring(0, sepIdx).trim
        val v = arg.substring(sepIdx + 1).trim
        if (v != "" && v != "Nan" && v != null) {
          cmdArgs.put(k, v)
          println(s"param $k = $v")
        }
      }
    }
    cmdArgs.toMap
  }


  def main(args: Array[String]): Unit = {

    val params = parse(args)

    val dataFormat = params.getOrElse("dataFormat", "libsvm")//libsvm,dummy
    val actionType = params.getOrElse("actionType", "train")
    val jsonFile = params.getOrElse("jsonFile", "")
    val input = params.getOrElse("data", "")
    val modelPath = params.getOrElse("modelPath", "")
    val predict = params.getOrElse("predictPath", "")
    val numBatch = params.getOrElse("numBatch", "10").toInt
    val maxIter = params.getOrElse("maxIter", "2").toInt
    val lr = params.getOrElse("lr", "0.1").toFloat
    val numField = params.getOrElse("numField", "13").toInt
    val task = params.getOrElse("task", "classification")

    val spark = SparkSession.builder()
      .appName("AngelClassification")
      .getOrCreate()

    val sparkConf = spark.sparkContext.getConf
    val driverCtx = DriverContext.get(sparkConf)
    driverCtx.startAngelAndPSAgent()


    val libsvm = spark.read.format("libsvmex")
    val dummy = spark.read.format("dummy")
    var trainData: DataFrame = null.asInstanceOf[DataFrame]

    if (dataFormat.equalsIgnoreCase("libsvm")) {
      trainData = libsvm.load(input)
    } else if (dataFormat.equalsIgnoreCase("dummy")){
      trainData = dummy.load(input)
    } else {
      throw new Exception(s"this dataFormat ${dataFormat} is not supported!")
    }

    if (actionType.equalsIgnoreCase("train")) {
      val classifier = {
        if (task.equalsIgnoreCase("classification"))
          new AngelClassifier()
            .setModelJsonFile(jsonFile)
            .setNumClass(2)
            .setNumBatch(numBatch)
            .setMaxIter(maxIter)
            .setLearningRate(lr)
            .setNumField(numField)
        else if (task.equalsIgnoreCase("regression"))
          new AngelRegressor()
            .setModelJsonFile(jsonFile)
            .setNumBatch(numBatch)
            .setMaxIter(maxIter)
            .setLearningRate(lr)
            .setNumField(numField)
        else {
          throw new Exception(s"task ${task} is not supported!")
        }

      }

      val model = classifier.fit(trainData)

      model.write.overwrite().save(modelPath)
    } else if (actionType.equalsIgnoreCase("predict")) {
      val predictor = {
        if (task.equalsIgnoreCase("classification"))
          AngelClassifierModel.read.load(modelPath)
        else if (task.equalsIgnoreCase("regression"))
          AngelRegressorModel.read.load(modelPath)
        else {
          throw new Exception(s"task ${task} is not supported!")
        }
      }
      val res = predictor.transform(trainData)
      res.write.mode("overwrite").save(predict)
    }

    driverCtx.stopAngelAndPSAgent()
    spark.close()

  }
}
