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
package org.apache.spark.angel.ml.tree.gbdt.example

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.angel.ml.tree.gbdt.GBDTConf._
import org.apache.spark.angel.ml.tree.gbdt.helper.FeatureImportance
import org.apache.spark.angel.ml.tree.gbdt.predict.GBDTPredictor
import org.apache.spark.angel.ml.tree.gbdt.train._
import org.apache.spark.angel.ml.tree.gbdt.tree.GBDTParam
import org.apache.spark.angel.ml.tree.objective.ObjectiveFactory
import org.apache.spark.angel.ml.tree.regression.RegTParam
import org.apache.spark.angel.ml.tree.util.MathUtil
import org.apache.spark.angel.ml.util.ArgsUtil

object GBDTExample {

  def main(args: Array[String]): Unit = {
    val params = ArgsUtil.parse(args)
    val actionType = params.getOrElse("actionType", "train")
    actionType match {
      case "train" => train(params)
      case "predict" => predict(params)
      case _ => throw new IllegalArgumentException("Unsupported action type: " + actionType)
    }
  }

  def train(params: Map[String, String]): Unit = {
    // spark conf
    @transient val conf = new SparkConf()
    require(!conf.getBoolean("spark.dynamicAllocation.enabled", false),
      "'spark.dynamicAllocation.enabled' should not be true")
    val numExecutors = conf.get("spark.executor.instances").toInt
    val numCores = conf.get("spark.executor.cores").toInt
    conf.set("spark.task.cpus", numCores.toString)
    conf.set("spark.locality.wait", "0")
    conf.set("spark.memory.fraction", "0.7")
    conf.set("spark.memory.storageFraction", "0.8")
    conf.set("spark.rpc.message.maxSize", "2000")
    conf.set("spark.scheduler.maxRegisteredResourcesWaitingTime", "600s")
    conf.set("spark.scheduler.minRegisteredResourcesRatio", "1.0")
    conf.set("spark.task.maxFailures", "1")
    conf.set("spark.yarn.maxAppAttempts", "1")
    conf.set("spark.network.timeout", "1000")
    conf.set("spark.executor.heartbeatInterval", "500")
    @transient implicit val sc = SparkContext.getOrCreate(conf)

    // parse hyper-parameters
    val isRegression = params.getOrElse(ML_TASK_TYPE, DEFAULT_ML_TASK_TYPE) match {
      case "classification" => false
      case "regression" => true
      case s => throw new IllegalArgumentException(s"Unsupported task type: $s, " +
        s"please input 'classification' or 'regression'")
    }
    val numClass = if (isRegression) 2 else params.getOrElse(ML_NUM_CLASS, DEFAULT_ML_NUM_CLASS.toString).toInt
    val (fullHessian, multiTree) = if (!isRegression && numClass > 2) {
      val fh = params.getOrElse(ML_GBDT_FULL_HESSIAN, DEFAULT_ML_GBDT_FULL_HESSIAN.toString).toBoolean
      val mt = params.getOrElse(ML_GBDT_MULTI_TREE, DEFAULT_ML_GBDT_MULTI_TREE.toString).toBoolean
      require(!(fh && mt), "For multi-class tasks, full-hessian and multi-tree are exclusive")
      (fh, mt)
    } else {
      (false, false)
    }
    val numFeature = params(ML_NUM_FEATURE).toInt
    val numRound = params.getOrElse(ML_GBDT_ROUND_NUM, DEFAULT_ML_GBDT_ROUND_NUM.toString).toInt
    val initLearningRate = params.getOrElse(ML_INIT_LEARN_RATE, DEFAULT_ML_INIT_LEARN_RATE.toString).toFloat
    val maxDepth = params.getOrElse(ML_GBDT_MAX_DEPTH, DEFAULT_ML_GBDT_MAX_DEPTH.toString).toInt
    val treeNodeNum = MathUtil.maxNodeNum(maxDepth)
    val maxNodeNum = params.getOrElse(ML_GBDT_MAX_NODE_NUM, treeNodeNum.toString).toInt min treeNodeNum
    val numSplit = params.getOrElse(ML_GBDT_SPLIT_NUM, DEFAULT_ML_GBDT_SPLIT_NUM.toString).toInt
    val minChildWeight = params.getOrElse(ML_GBDT_MIN_CHILD_WEIGHT, DEFAULT_ML_GBDT_MIN_CHILD_WEIGHT.toString).toFloat
    val leafwise = params.getOrElse(ML_GBDT_LEAF_WISE, DEFAULT_ML_GBDT_LEAF_WISE.toString).toBoolean
    val minNodeInstance = params.getOrElse(ML_GBDT_MIN_NODE_INSTANCE, DEFAULT_ML_GBDT_MIN_NODE_INSTANCE.toString).toInt
    val minSplitGain = params.getOrElse(ML_GBDT_MIN_SPLIT_GAIN, DEFAULT_ML_GBDT_MIN_SPLIT_GAIN.toString).toFloat
    val regAlpha = params.getOrElse(ML_GBDT_REG_ALPHA, DEFAULT_ML_GBDT_REG_ALPHA.toString).toFloat
    val regLambda = params.getOrElse(ML_GBDT_REG_LAMBDA, DEFAULT_ML_GBDT_REG_LAMBDA.toString).toFloat max 1.0f
    val maxLeafWeight = params.getOrElse(ML_GBDT_MAX_LEAF_WEIGHT, DEFAULT_ML_GBDT_MAX_LEAF_WEIGHT.toString).toFloat
    val insSampleRatio = params.getOrElse(ML_INSTANCE_SAMPLE_RATIO, DEFAULT_ML_INSTANCE_SAMPLE_RATIO.toString).toFloat
    val featSampleRatio = params.getOrElse(ML_FEATURE_SAMPLE_RATIO, DEFAULT_ML_FEATURE_SAMPLE_RATIO.toString).toFloat
    val (lossFunc, evalMetrics) = if (isRegression) {
      // for regression task, use RMSE loss
      ("rmse", Array("rmse"))
    } else {
      // get loss function
      val loss = params(ML_LOSS_FUNCTION)
      // ensure that the loss function fits #class & get default eval metric
      val defaultMetric = (if (numClass == 2) ObjectiveFactory.getBinaryLoss(loss)
      else ObjectiveFactory.getMultiLoss(loss)).defaultEvalMetric().toString
      // get eval metric
      var metrics = params.getOrElse(ML_EVAL_METRIC, defaultMetric)
        .split(",").map(_.trim).filter(_.nonEmpty)
      // we may schedule learning rate w.r.t. to default metric
      metrics = defaultMetric +: metrics.filter(_ != defaultMetric)
      // ensure that all evaluation metrics fit #class
      val labels = Array.ofDim[Float](1)
      val preds = Array.ofDim[Float](labels.length * (if (numClass == 2) 1 else numClass))
      ObjectiveFactory.getEvalMetrics(metrics).foreach(_.eval(preds, labels))
      (loss, metrics)
    }

    // RegTParam
    val regTParam = new RegTParam()
      .setLeafwise(leafwise)
      .setMinSplitGain(minSplitGain)
      .setMinNodeInstance(minNodeInstance)
    // TParam
    regTParam.setNumFeature(numFeature)
      .setMaxDepth(maxDepth)
      .setMaxNodeNum(maxNodeNum)
      .setNumSplit(numSplit)
      .setInsSampleRatio(insSampleRatio)
      .setFeatSampleRatio(featSampleRatio)
    // GBDTParam
    val param = new GBDTParam(regTParam)
      .setRegression(isRegression)
      .setNumClass(numClass)
      .setFullHessian(fullHessian)
      .setMultiTree(multiTree)
      .setNumRound(numRound)
      .setInitLearningRate(initLearningRate)
      .setMinChildWeight(minChildWeight)
      .setRegAlpha(regAlpha)
      .setRegLambda(regLambda)
      .setMaxLeafWeight(maxLeafWeight)
      .setLossFunc(lossFunc)
      .setEvalMetrics(evalMetrics)
    println(s"Hyper-parameters:\n$param")
    val trainer = params.getOrElse(ML_PARALLEL_MODE, DEFAULT_ML_PARALLEL_MODE) match {
      case GBDTTrainer.FEATURE_PARALLEL_MODE =>
        println("Training in feature parallel mode")
        new FPGBDTTrainer(param)
      case GBDTTrainer.DATA_PARALLEL_MODE =>
        println("Training in data parallel mode")
        new DPGBDTTrainer(param)
      case mode =>
        throw new IllegalArgumentException(s"Unrecognizable parallel mode: $mode")
    }

    val trainInput = params(ML_TRAIN_DATA_PATH)
    val validInput = params(ML_VALID_DATA_PATH)
    val modelPath = params(ML_MODEL_PATH)
    val importanceType = params.getOrElse(ML_GBDT_IMPORTANCE_TYPE, DEFAULT_ML_GBDT_IMPORTANCE_TYPE)
    FeatureImportance.ensureImportanceType(importanceType)

    val model = trainer.fit(trainInput, validInput, numExecutors, numCores)
    GBDTTrainer.save(model, modelPath, importanceType)
  }

  def predict(params: Map[String, String]): Unit = {
    @transient val conf = new SparkConf()
    conf.set("spark.rpc.message.maxSize", "2000")
    conf.set("spark.driver.maxResultSize", "2G")
    @transient implicit val sc = SparkContext.getOrCreate(conf)

    val modelPath = params(ML_MODEL_PATH)
    val inputPath = params(ML_PREDICT_INPUT_PATH)
    val outputPath = params(ML_PREDICT_OUTPUT_PATH)

    val model = GBDTPredictor.loadModel(modelPath)
    GBDTPredictor.predict(model, inputPath, outputPath)
  }
}
