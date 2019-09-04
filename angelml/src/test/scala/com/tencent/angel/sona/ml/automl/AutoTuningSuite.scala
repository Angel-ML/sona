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

package com.tencent.angel.sona.ml.automl
import com.tencent.angel.sona.ml.classification.AngelClassifier
import com.tencent.angel.sona.ml.util.AngelTestUtils
import com.tencent.angel.spark.automl.tuner.TunerParam
import com.tencent.angel.spark.automl.tuner.config.{Configuration, ConfigurationSpace}
import com.tencent.angel.spark.automl.tuner.parameter.{ParamConfig, ParamParser}
import com.tencent.angel.spark.automl.tuner.solver.Solver

import scala.collection.mutable
import scala.util.Random

class AutoTuningSuite extends AngelTestUtils {

  def parseConfig(input: String)(implicit solver: Solver): this.type = {
    val paramConfigs: Array[ParamConfig] = ParamParser.parse(input)
    paramConfigs.foreach{ config =>
      assert(TunedParams.exists(config.getParamName),
        s"${config.getParamName} not supported in tuning")
      addParam(solver, config.getParamName, config.getParamType, config.getValueType,
        config.getParamRange, Random.nextInt())
    }
    this
  }

  def addParam(solver: Solver, pName: String, pType: String, vType: String, config: String, seed: Int = 100): this.type = {
    solver.addParam(pName, pType, vType, config, seed)
    this
  }

  test("fm_train") {

    val trainData = dummy.load("./data/angel/census/census_148d_train.dummy")

    val tuneIter = 20
    val minimize = false
    val surrogate = "GaussianProcess"
    val config = "learningRate|C|double|0.1:1:100#maxIter|D|float|1:5:1"

    // init solver
    val cs: ConfigurationSpace = new ConfigurationSpace("cs")
    implicit val solver: Solver = Solver(cs, minimize, surrogate)
    parseConfig(config)
    TunerParam.setBatchSize(1)

    val classifier = new AngelClassifier()
      .setModelJsonFile("./angelml/src/test/jsons/deepfm.json")

    // iterate tuning
    (0 until tuneIter).foreach { iter =>
      println(s"==========Tuner Iteration[$iter]==========")
      val configs: Array[Configuration] = solver.suggest
      for (config <- configs) {
        val paramMap: mutable.Map[String, Double] = new mutable.HashMap[String, Double]()
        for (paramType <- solver.getParamTypes) {
          paramMap += (paramType._1 -> config.get(paramType._1))
        }
        val numBatch = paramMap.getOrElse("numBatch", 10.toDouble).toInt
        val maxIter = paramMap.getOrElse("maxIter", 5.toDouble).toInt
        val lr = paramMap.getOrElse("learningRate", 0.1)
        val decayAlpha = paramMap.getOrElse("decayAlpha", 0.001)
        val decayBeta = paramMap.getOrElse("decayBeta", 0.001)
        val decayIntervals = paramMap.getOrElse("decayIntervals", 100.0).toInt

        val classifier = new AngelClassifier()
          .setModelJsonFile("./angelml/src/test/jsons/deepfm.json")
          .setNumClass(2)
          .setNumField(13)
          .setNumBatch(numBatch)
          .setMaxIter(maxIter)
          .setLearningRate(lr)
          .setDecayAlpha(decayAlpha)
          .setDecayBeta(decayBeta)
          .setDecayIntervals(decayIntervals)

        val model = classifier.fit(trainData)

        val metric = model.evaluate(trainData).accuracy
        println(s"metric at iteration $iter = $metric, best ever = ${solver.optimal._2}")

        if ((minimize && metric < solver.optimal._2)
          || (!minimize && metric > solver.optimal._2)) {
          println(s"find a better configuration = ${config.getVector.toArray.mkString("|")}, " +
            s"metric = $metric")
          model.write.overwrite().save("./trained_models/fm")
        }
        solver.feed(config, metric)
      }
    }
  }
}
