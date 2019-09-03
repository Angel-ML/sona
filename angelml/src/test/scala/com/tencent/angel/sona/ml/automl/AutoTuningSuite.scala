package com.tencent.angel.sona.ml.automl

import com.tencent.angel.sona.examples.AutoJsonRunnerExamples.parseConfig
import com.tencent.angel.sona.ml.classification.AngelClassifier
import com.tencent.angel.sona.ml.util.AngelTestUtils
import com.tencent.angel.spark.automl.tuner.TunerParam
import com.tencent.angel.spark.automl.tuner.config.{Configuration, ConfigurationSpace}
import com.tencent.angel.spark.automl.tuner.solver.Solver

import scala.collection.mutable

class AutoTuningSuite extends AngelTestUtils {

  test("fm_train") {

    val trainData = dummy.load("./data/angel/census/census_148d_train.dummy")

    val tuneIter = 20
    val minimize = false
    val surrogate = "GaussianProcess"
    val config = "learningRate|C|double|0.1:1:100#maxIter|D|float|10:50:5"

    // init solver
    val cs: ConfigurationSpace = new ConfigurationSpace("cs")
    implicit val solver: Solver = Solver(cs, minimize, surrogate)
    parseConfig(config)
    TunerParam.setBatchSize(1)

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
        val maxIter = paramMap.getOrElse("maxIter", 50.toDouble).toInt
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
        solver.feed(config, metric)

        if ((minimize && metric < solver.optimal._2)
          || (!minimize && metric > solver.optimal._2)) {
          println(s"find a better configuration = ${config.getVector.toArray.mkString("@")}, " +
            s"metric = $metric")
          model.write.overwrite().save("trained_models/fm")
        }
      }
    }
  }
}
