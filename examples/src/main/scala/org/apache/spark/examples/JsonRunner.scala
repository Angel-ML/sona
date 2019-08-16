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
package org.apache.spark.examples

import java.io.File
import java.nio.file.Paths

import com.tencent.angel.ps.PSContext
import com.tencent.angel.sona.core.DriverContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrameReader, SparkSession}
import org.apache.spark.angelml.classification.AngelClassifier
import org.apache.spark.angelml.feature.LabeledPoint
import org.apache.spark.angelml.linalg.Vectors

import scala.collection.mutable


object JsonRunner {

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

    val defaultInput = "hdfs://tl-nn-tdw.tencent-distribute.com:54310/user/tdw_rachelsun/joyjxu/angel-test/daw_data/census_148d_train.libsvm"
    val defaultOutput = "hdfs://tl-nn-tdw.tencent-distribute.com:54310/user/tdw_rachelsun/joyjxu/trained_models"
    val defaultJsonFile = "No json file parsed..."
    val defaultDataFormat = "org.apache.spark.ml.source.libsvm.LibSVMFileFormat"

    val params = parse(args)

    val DataFormat = params.getOrElse("DataFormat", defaultDataFormat)
    val JsonFile = params.getOrElse("JsonFile", defaultJsonFile)
    val Input = params.getOrElse("Input", defaultInput)
    val Output = params.getOrElse("Output", defaultOutput)

    val spark = SparkSession.builder()
      .master("yarn-cluster")
      .appName("AngelClassification")
      .getOrCreate()

    val sparkConf = spark.sparkContext.getConf
    val driverCtx = DriverContext.get(sparkConf)

    driverCtx.startAngelAndPSAgent()

    val trainData = if ( DataFormat == "dense") { // use dense data
      val Data = spark.sparkContext.textFile(Input).map(_.split(" "))
      val Data_ = Data.map(x => LabeledPoint(x(0).toDouble, Vectors.dense(x.slice(1, x.length-1).map(_.toDouble))))
      spark.createDataFrame(Data_)
    } else {
      val reader = spark.read.format(DataFormat)
      reader.load(Input)
    }

    val classifier = new AngelClassifier()
      .setModelJsonFile(JsonFile)
      .setNumClass(2)
      .setNumBatch(1000)

    val model = classifier.fit(trainData)

    model.write.overwrite.save(Output)

    driverCtx.stopAngelAndPSAgent()

  }
}