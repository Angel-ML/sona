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
package com.tencent.angel.sona.tree.gbdt.predict

import com.tencent.angel.sona.tree.gbdt.GBDTConf._
import com.tencent.angel.sona.tree.gbdt.GBDTModel
import com.tencent.angel.sona.tree.util.DataLoader
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.util.SparkUtil

object GBDTPredictor {

  def main(args: Array[String]): Unit = {
    @transient val conf = new SparkConf()
    conf.set("spark.rpc.message.maxSize", "2000")
    conf.set("spark.driver.maxResultSize", "2G")
    @transient implicit val sc = SparkContext.getOrCreate(conf)

    val params = SparkUtil.parse(args)
    val modelPath = params(ML_MODEL_PATH)
    val inputPath = params(ML_PREDICT_INPUT_PATH)
    val outputPath = params(ML_PREDICT_OUTPUT_PATH)

    val model = loadModel(modelPath)
    predict(model, inputPath, outputPath)
  }

  def loadModel(modelFolder: String)(implicit sc: SparkContext): GBDTModel = {
    val loadStart = System.currentTimeMillis()
    val modelPath = modelFolder + "/model"
    println(s"Loading model from $modelPath...")
    val model = sc.objectFile[GBDTModel](modelPath).first()
    println(s"Loading model with ${model.numTree} tree(s) done, " +
      s"cost ${System.currentTimeMillis() - loadStart} ms")
    model
  }

  def predict(model: GBDTModel, input: String, output: String)
             (implicit sc: SparkContext): Unit = {
    val predStart = System.currentTimeMillis()
    println("Start to do prediction...")
    println(s"Prediction input: $input")
    println(s"Prediction output: $output")
    val predictor = new GBDTPredictor(model)
    val preds = if (predictor.isRegression) {
      predictor.predictRegression(input)
        .map(x => s"${x._1} ${x._2}")
    } else {
      predictor.predictClassification(input)
        .map(x => s"${x._1} ${x._2} ${x._3.mkString(",")}")
    }
    preds.saveAsTextFile(output)
    println(s"Prediction done, cost ${System.currentTimeMillis() - predStart} ms")
  }

  private def predictRaw(model: GBDTModel, ins: Vector): Array[Float] = {
    model.predict(ins)
  }

  private def predToClass(predRaw: Array[Float]): Int = {
    predRaw.length match {
      case 1 => if (predRaw.head > 0.0f) 1 else 0
      case _ => predRaw.zipWithIndex.maxBy(_._1)._2
    }
  }
}

class GBDTPredictor(model: GBDTModel) {
  import GBDTPredictor._

  def predictRegression(input: String)
                       (implicit sc: SparkContext): RDD[(Long, Float)] = {
    require(model.param.isRegression, "Input model is obtained " +
      "from a classification task, cannot be used in regression")
    val maxDim = model.param.regTParam.numFeature
    val bcModel = sc.broadcast(model)
    DataLoader.loadLibsvm(input, maxDim)
      .map {
        case (id, ins) =>
          val predRaw = predictRaw(bcModel.value, ins)
          (id.toLong, predRaw.head)
      }
  }

  def predictClassification(input: String)
                           (implicit sc: SparkContext): RDD[(Long, Int, Array[Float])] = {
    require(!model.param.isRegression, "Input model is obtained " +
      "from a regression task, cannot be used in classification")
    val maxDim = model.param.regTParam.numFeature
    val bcModel = sc.broadcast(model)
    DataLoader.loadLibsvm(input, maxDim)
      .map {
        case (id, ins) =>
          val predRaw = predictRaw(bcModel.value, ins)
          val predClass = predToClass(predRaw)
          (id.toLong, predClass, predRaw)
      }
  }

  def isRegression: Boolean = model.param.isRegression
}
