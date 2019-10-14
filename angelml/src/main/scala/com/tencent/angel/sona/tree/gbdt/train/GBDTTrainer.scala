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
package com.tencent.angel.sona.tree.gbdt.train

import com.tencent.angel.exception.AngelException
import com.tencent.angel.sona.tree.gbdt.GBDTConf._
import org.apache.hadoop.fs.Path
import com.tencent.angel.sona.tree.gbdt.GBDTModel
import com.tencent.angel.sona.tree.gbdt.GBDTModel.GBTTree
import com.tencent.angel.sona.tree.gbdt.dataset.Dataset
import com.tencent.angel.sona.tree.gbdt.helper._
import com.tencent.angel.sona.tree.gbdt.histogram.GradPair
import com.tencent.angel.sona.tree.gbdt.metadata.{FeatureInfo, InstanceInfo}
import com.tencent.angel.sona.tree.gbdt.tree.{GBDTParam, GBTNode, GBTSplit}
import com.tencent.angel.sona.tree.objective.ObjectiveFactory
import com.tencent.angel.sona.tree.objective.metric.EvalMetric
import com.tencent.angel.sona.tree.regression.RegTParam
import com.tencent.angel.sona.tree.stats.hash.Mix64Hash
import com.tencent.angel.sona.tree.stats.quantile.QuantileSketch
import com.tencent.angel.sona.tree.util.MathUtil
import org.apache.spark.{SparkConf, SparkContext, SparkEnv}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.util.SparkUtil

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ArrayBuilder => AB}

object GBDTTrainer {
  private[gbdt] val DATA_PARALLEL_MODE = "dp"
  private[gbdt] val FEATURE_PARALLEL_MODE = "fp"
  private[gbdt] val DEFAULT_PARALLEL_MODE = FEATURE_PARALLEL_MODE
  // TODO: add 'auto' parallel mode

  def main(args: Array[String]): Unit = {
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
    val params = SparkUtil.parse(args)
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
      case FEATURE_PARALLEL_MODE =>
        println("Training in feature parallel mode")
        new FPGBDTTrainer(param)
      case DATA_PARALLEL_MODE =>
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

    val path = new Path(modelPath)
    val fs = path.getFileSystem(sc.hadoopConfiguration)
    if (fs.exists(path) && !params.getOrElse(ML_DELETE_IF_EXISTS, "true").toBoolean) {
      throw new AngelException(s"Output path $modelPath already exists, " +
        s"please delete it or set '$ML_DELETE_IF_EXISTS' as true to overwrite")
    }

    val model = trainer.fit(trainInput, validInput, numExecutors, numCores,
      params.getOrElse(ML_GBDT_INIT_TWO_ROUND, "false").toBoolean)
    save(model, modelPath, importanceType,
      params.getOrElse(ML_DELETE_IF_EXISTS, "true").toBoolean)
  }

  def save(model: GBDTModel, basePath: String,
           importanceType: String, deleteIfExist: Boolean = true)
          (implicit sc: SparkContext): Unit = {
    val path = new Path(basePath)
    val fs = path.getFileSystem(sc.hadoopConfiguration)
    if (fs.exists(path)) {
      if (deleteIfExist) {
        println(s"Output path $basePath already exists, deleting...")
        fs.delete(path, true)
      } else {
        throw new AngelException(s"Output path $basePath already exists, " +
          s"please delete it or set '$ML_DELETE_IF_EXISTS' as true to overwrite")
      }
    }

    val modelPath = basePath + "/model"
    println(s"Saving model to $modelPath")
    sc.parallelize(Seq(model), numSlices = 1).saveAsObjectFile(modelPath)

    val featurePath = basePath + "/feature_importance"
    println(s"Saving feature importance ($importanceType) to $featurePath")

    sc.parallelize(FeatureImportance.featImportance(model, importanceType)
      .map(pair => s"${pair._1} ${pair._2}"),
      numSlices = 1).saveAsTextFile(featurePath)
  }

  private[gbdt] def hashFeatureGrouping(numFeature: Int, numGroup: Int): (Array[Int], Array[Array[Int]]) = {
    val fidToGroupId = Array.ofDim[Int](numFeature)
    val buffers = Array.ofDim[AB.ofInt](numGroup)
    for (partId <- 0 until numGroup) {
      buffers(partId) = new AB.ofInt
      buffers(partId).sizeHint((1.5 * numFeature / numGroup).toInt)
    }
    val hasFunc = new Mix64Hash(numGroup)
    for (fid <- 0 until numFeature) {
      val groupId = hasFunc.hash(fid) % numGroup
      fidToGroupId(fid) = groupId
      buffers(groupId) += fid
    }
    val groupIdToFid = buffers.map(_.result())
    (fidToGroupId, groupIdToFid)
  }

  private[gbdt] def getSplits(sketch: QuantileSketch, numSplit: Int): (Boolean, Array[Float], Int) = {
    // return (isCategorical, candidate splits, nnz)
    if (sketch == null || sketch.isEmpty) {
      (false, null, 0)
    } else {
      val nnz = sketch.getN.toInt
      val distinct = sketch.tryDistinct(FeatureInfo.ENUM_THRESHOLD)
      if (distinct != null) {
        (true, distinct, nnz)
      } else {
        val tmp = MathUtil.unique(sketch.getQuantiles(numSplit))
        if (tmp.length > 1) {
          (false, tmp, nnz)
        } else {
          val s = tmp.head
          if (s > 0) (false, Array(0, s), nnz)
          else if (s < 0) (false, Array(s, 0), nnz)
          else (false, Array(0, MathUtil.EPSILON), nnz)
        }
      }
    }
  }
}

abstract class GBDTTrainer(param: GBDTParam) {

  def fit(trainInput: String, validInput: String,
          numWorker: Int, numThread: Int,
          initTwoRound: Boolean = false)
         (implicit sc: SparkContext): GBDTModel = {
    println(s"Training data path: $trainInput")
    println(s"Validation data path: $validInput")
    println(s"#workers[$numWorker], #threads[$numThread]")
    if (initTwoRound)
      initializeTwoRound(trainInput, validInput, numWorker, numThread)
    else
      initialize(trainInput, validInput, numWorker, numThread)
    val model = train()
    shutdown()
    model
  }

  protected def initialize(trainInput: String, validInput: String,
                           numWorker: Int, numThread: Int)
                          (implicit sc: SparkContext): Unit = ???

  protected def initializeTwoRound(trainInput: String, validInput: String,
                                   numWorker: Int, numThread: Int)
                                  (implicit sc: SparkContext): Unit = ???

  protected def shutdown(): Unit = ???

  protected def train()(implicit sc: SparkContext): GBDTModel = {
    println("Start to train a GBDT model")
    val model = new GBDTModel(param)
    val trainStart = System.currentTimeMillis()

    // tree/node meta data
    val numTreePerRound = if (param.isMultiClassMultiTree) param.numClass else 1
    val maxInnerNum = MathUtil.maxInnerNodeNum(param.regTParam.maxDepth)
    val activeNodes = ArrayBuffer[Int]()
    val toSplits = mutable.Map[Int, GBTSplit]()
    val nodeSizes = Array.ofDim[Int](MathUtil.maxNodeNum(param.regTParam.maxDepth))
    // lr scheduler
    val lrScheduler = if (!sc.getConf.getBoolean(ML_REDUCE_LR_ON_PLATEAU,
      DEFAULT_ML_REDUCE_LR_ON_PLATEAU)) {
      new LRScheduler(param.numRound, param.evalMetrics, patient = -1)
    } else {
      val patient = sc.getConf.getInt(ML_REDUCE_LR_ON_PLATEAU_PATIENT,
        DEFAULT_ML_REDUCE_LR_ON_PLATEAU_PATIENT)
      val threshold = sc.getConf.getDouble(ML_REDUCE_LR_ON_PLATEAU_THRESHOLD,
        DEFAULT_ML_REDUCE_LR_ON_PLATEAU_THRESHOLD.toDouble).toFloat
      val decayFactor = sc.getConf.getDouble(ML_REDUCE_LR_ON_PLATEAU_DECAY_FACTOR,
        DEFAULT_ML_REDUCE_LR_ON_PLATEAU_DECAY_FACTOR.toDouble).toFloat
      val earlyStop = sc.getConf.getInt(ML_REDUCE_LR_ON_PLATEAU_EARLY_STOP,
        DEFAULT_ML_REDUCE_LR_ON_PLATEAU_EARLY_STOP)
      println(s"Train with ReduceLROnPlateau strategy: patient[$patient] " +
        s"threshold[$threshold] decay factor[$decayFactor] early stop[$earlyStop]")
      new LRScheduler(param.numRound, param.evalMetrics, patient = patient,
        threshold = threshold, decayFactor = decayFactor, earlyStop = earlyStop)
    }

    var currentLR = param.initLearningRate
    var earlyStop = false
    var round = 0
    while (round < param.numRound && !earlyStop) {
      println(s"Start to train round[${round + 1}]")
      val roundStart = System.currentTimeMillis()

      // 1. calc grad pairs
      calcGradPairs()
      // 2. train trees in current round
      for (treeId <- 0 until numTreePerRound) {
        val treeStart = System.currentTimeMillis()
        activeNodes.clear()
        toSplits.clear()
        for (nid <- nodeSizes.indices)
          nodeSizes(nid) = -1
        val classIdOpt = if (numTreePerRound > 1) Some(treeId) else None

        // 2.1. create new tree, reset workers
        val tree = createNewTree(nodeSizes, classIdOpt = classIdOpt)
        // 2.2. iteratively build one tree
        activeNodes += 0
        while (tree.size() + 2 < param.regTParam.maxNodeNum
          && (activeNodes.nonEmpty || toSplits.nonEmpty)) {
          // 2.2.1. build histograms and find splits
          if (activeNodes.nonEmpty) {
            findSplits(tree, activeNodes, nodeSizes,
              classIdOpt = classIdOpt).foreach { case (nid, split) => toSplits(nid) = split }
          }
          val splitNodeNum = if (param.regTParam.leafwise) 1 else Math.min(
            toSplits.size, (param.regTParam.maxNodeNum - tree.size()) / 2)
          val (toSplit, toSetLeaf) = chooseSplits(tree, toSplits, activeNodes, splitNodeNum)
          activeNodes.clear()
          // 2.2.2. set leaves
          setAsLeaves(tree, toSetLeaf)
          // 2.2.3. split nodes
          if (toSplit.nonEmpty) {
            splitNodes(tree, toSplit, nodeSizes, classIdOpt = classIdOpt)
            toSplit.foreach {
              case (nid, _) =>
                if (2 * nid + 1 < maxInnerNum) {
                  activeNodes += 2 * nid + 1
                  activeNodes += 2 * nid + 2
                }
            }
          }
        }
        // 2.3. finish current tree & update preds
        finishTree(tree, currentLR, classIdOpt = classIdOpt)
        model.add(tree, currentLR)
        println(s"Train tree[${treeId + 1}/$numTreePerRound] of round[${round + 1}] " +
          s"with ${tree.size()} nodes (${(tree.size() - 1) / 2 + 1} leaves) " +
          s"cost ${System.currentTimeMillis() - treeStart} ms, " +
          s"${System.currentTimeMillis() - trainStart} ms elapsed")
      }
      // 3. evaluation
      val metrics = evaluate(round, lrScheduler)
      // 4. finish training of current round
      println(s"Train round[${round + 1}] cost ${System.currentTimeMillis() - roundStart} ms, " +
        s"${System.currentTimeMillis() - trainStart} ms elapsed")
      round += 1
      // 5. schedule learning rate or early stop
      currentLR = lrScheduler.step(metrics, currentLR)
      earlyStop = currentLR < MathUtil.EPSILON
    }

    if (sc.getConf.getBoolean(ML_GBDT_BEST_CHECKPOINT, DEFAULT_ML_GBDT_BEST_CHECKPOINT)) {
      println(s"Train model cost ${System.currentTimeMillis() - trainStart} ms")
      val best = lrScheduler.getBest
      model.keepFirstTrees(best._2 * numTreePerRound)
      println(s"Best performance occurs in round[${best._2}] with ${best._1}[${best._3}], " +
        s"so the final model contains ${model.numTree} trees")
    } else {
      println(s"Train model with ${model.numTree} " +
        s"cost ${System.currentTimeMillis() - trainStart} ms")
    }
    model
  }

  protected def calcGradPairs(): Unit = {
    val calcStart = System.currentTimeMillis()
    doCalcGradPairs()
    // println(s"Calc gradients cost ${System.currentTimeMillis() - calcStart} ms")
  }

  protected def createNewTree(nodeSizes: Array[Int], classIdOpt: Option[Int] = None): GBTTree = {
    val createStart = System.currentTimeMillis()
    val tree = new GBTTree(param.regTParam)
    val root = new GBTNode(0, null)
    tree.setRoot(root)
    val (rootSize, rootGradPair) = doNewTree(classIdOpt = classIdOpt)
    nodeSizes(0) = rootSize
    root.setSumGradPair(rootGradPair)
    root.calcGain(param)
    // println(s"Create new tree cost ${System.currentTimeMillis() - createStart} ms")
    tree
  }

  protected def findSplits(tree: GBTTree, nids: Seq[Int], nodeSizes: Seq[Int],
                           classIdOpt: Option[Int] = None): Map[Int, GBTSplit] = {
    val findStart = System.currentTimeMillis()

    def nodeCanSplit(nid: Int): Boolean = {
      nodeSizes(nid) > param.regTParam.minNodeInstance &&
        tree.getNode(nid).getSumGradPair.satisfyWeight(param)
    }

    val splits = if (nids.head == 0) {
      if (!nodeCanSplit(0)) {
        Map.empty.asInstanceOf[Map[Int, GBTSplit]]
      } else {
        val rootSplit = doFindRootSplit(tree.getRoot.getSumGradPair, classIdOpt = classIdOpt)
        if (rootSplit != null) Map(0 -> rootSplit)
        else Map.empty.asInstanceOf[Map[Int, GBTSplit]]
      }
    } else {
      val canSplits = nids.map(nodeCanSplit)
      val nodeGradPairs = nids.map(nid => tree.getNode(nid).getSumGradPair)
      val (toBuild, toSubtract, toRemove) = HistBuilder.buildScheme(
        nids, canSplits, nodeSizes, param)
      doFindSplits(nids, nodeGradPairs, canSplits, toBuild, toSubtract, toRemove,
        classIdOpt = classIdOpt)
    }

    // println(s"Build histograms and find best splits cost " +
    //   s"${System.currentTimeMillis() - findStart} ms")

    splits
  }

  protected def chooseSplits(tree: GBTTree, toSplits: mutable.Map[Int, GBTSplit], activeNodes: Seq[Int],
                             splitNodeNum: Int): (Seq[(Int, GBTSplit)], Seq[Int]) = {
    val leaves = activeNodes.filter(nid => !toSplits.contains(nid))
    if (splitNodeNum < toSplits.size) {
      val sorted = toSplits.toSeq.sortBy(-_._2.getSplitEntry.getGain)
      val top = sorted.slice(0, splitNodeNum)
      top.foreach{ case (nid, _) => toSplits.remove(nid) }
      (top, leaves)
    } else {
      val all = toSplits.toSeq
      toSplits.clear()
      (all, leaves)
    }
  }

  protected def splitNodes(tree: GBTTree, toSplit: Seq[(Int, GBTSplit)], nodeSizes: Array[Int],
                           classIdOpt: Option[Int] = None): Unit = {
    val splitStart = System.currentTimeMillis()
    toSplit.foreach {
      case (nid, split) =>
        val node = tree.getNode(nid)
        node.setSplitEntry(split.getSplitEntry)
        val leftChild = new GBTNode(2 * nid + 1, node)
        val rightChild = new GBTNode(2 * nid + 2, node)
        tree.setNode(2 * nid + 1, leftChild)
        tree.setNode(2 * nid + 2, rightChild)
        node.setLeftChild(leftChild)
        node.setRightChild(rightChild)
        leftChild.setSumGradPair(split.getLeftGradPair)
        rightChild.setSumGradPair(split.getRightGradPair)
        leftChild.calcGain(param)
        rightChild.calcGain(param)
    }
    doSplitNodes(toSplit, classIdOpt = classIdOpt).foreach {
      case (nid, nodeSize) =>
        require(nodeSizes(nid) == -1)
        nodeSizes(nid) = nodeSize
    }
    // println(s"Split nodes cost ${System.currentTimeMillis() - splitStart} ms")
  }

  protected def setAsLeaves(tree: GBTTree, nids: Seq[Int]): Unit = {
    doSetAsLeaves(nids)
    nids.foreach(nid => {
      tree.getNode(nid).chgToLeaf()
    })
  }

  protected def finishTree(tree: GBTTree, learningRate: Float,
                           classIdOpt: Option[Int] = None): Unit = {
    val finishStart = System.currentTimeMillis()
    tree.getNodes.foreach {
      case (_, node) =>
        if (node.getSplitEntry == null && !node.isLeaf)
          node.chgToLeaf()
        if (node.isLeaf) {
          if (!param.isLeafVector)
            node.calcWeight(param)
          else
            node.calcWeights(param)
        }
    }
    doFinishTree(tree, learningRate, classIdOpt = classIdOpt)
    // println(s"Finish tree cost ${System.currentTimeMillis() - finishStart} ms")
  }

  protected def evaluate(round: Int, lrScheduler: LRScheduler)
  : Seq[(EvalMetric.Kind, Double, Double)] = {
    val metrics = doEvaluate()
    val trainEvalMsg = metrics.map {
      case (kind, train, _) => s"$kind[$train]"
    }.mkString(", ")
    val validEvalMsg = metrics.map {
      case (kind, _, valid) => s"$kind[$valid]"
    }.mkString(", ")
    println(s"Evaluation on train data after ${round + 1} round(s): $trainEvalMsg")
    println(s"Evaluation on valid data after ${round + 1} round(s): $validEvalMsg")
    metrics
  }

  /////////////////////////////////////////////////////////////
  //                       Worker APIs                       //
  /////////////////////////////////////////////////////////////
  protected def doCalcGradPairs(): Unit = ???

  protected def doNewTree(classIdOpt: Option[Int] = None): (Int, GradPair) = ???

  protected def doFindRootSplit(rootGradPair: GradPair, classIdOpt: Option[Int] = None): GBTSplit = ???

  protected def doFindSplits(nids: Seq[Int], nodeGradPairs: Seq[GradPair],
                             canSplits: Seq[Boolean], toBuild: Seq[Int],
                             toSubtract: Seq[Boolean], toRemove: Seq[Int],
                             classIdOpt: Option[Int] = None): Map[Int, GBTSplit] = ???

  protected def doSplitNodes(splits: Seq[(Int, GBTSplit)],
                             classIdOpt: Option[Int] = None): Seq[(Int, Int)] = ???

  protected def doSetAsLeaves(nids: Seq[Int]): Unit = ???

  protected def doFinishTree(tree: GBTTree, learningRate: Float,
                             classIdOpt: Option[Int] = None): Unit = ???

  protected def doEvaluate(): Seq[(EvalMetric.Kind, Double, Double)] = ???
}


private[gbdt] abstract class GBDTWorker(val workerId: Int, val param: GBDTParam) {
  @transient private[gbdt] var trainLabels: Array[Float] = _
  @transient private[gbdt] var trainData: Dataset[Int, Int] = _
  @transient private[gbdt] var validLabels: Array[Float] = _
  @transient private[gbdt] var validData: Array[Vector] = _
  @transient private[gbdt] var validPreds: Array[Float] = _

  private[gbdt] lazy val numTrain: Int = trainData.size
  private[gbdt] lazy val numValid: Int = validData.length

  @transient private[gbdt] var insInfo: InstanceInfo = _
  @transient private[gbdt] var featInfo: FeatureInfo = _

  @transient private[gbdt] var nodeIndexer: NodeIndexer = _
  @transient private[gbdt] var histManager: HistManager = _
  @transient private[gbdt] var histBuilder: HistBuilder = _
  @transient private[gbdt] var splitFinder: SplitFinder = _

  private[gbdt] val loss = ObjectiveFactory.getLoss(param.lossFunc)
  private[gbdt] val evalMetrics = ObjectiveFactory.getEvalMetricsOrDefault(param.evalMetrics, loss)

  private[gbdt] def initialize(trainLabels: Array[Float], trainData: Dataset[Int, Int],
                               validLabels: Array[Float], validData: Array[Vector],
                               insInfo: InstanceInfo, featInfo: FeatureInfo): Unit = {
    this.trainLabels = trainLabels
    this.trainData = trainData
    this.validLabels = validLabels
    this.validData = validData
    this.validPreds = Array.ofDim[Float](validData.length *
      (if (param.numClass == 2) 1 else param.numClass))
    this.insInfo = insInfo
    this.featInfo = featInfo
    this.nodeIndexer = NodeIndexer(param.regTParam.maxDepth, trainData.size)
    this.histManager = HistManager(param, featInfo)
    this.histBuilder = HistBuilder(param, insInfo, featInfo)
    this.splitFinder = SplitFinder(param, featInfo)
    this.histManager.reset(featInfo.isFeatUsed)
  }

  private[gbdt] def calcGradPairs(): Unit = {
    val rootGradPair = insInfo.calcGradPairs(trainLabels,
      ObjectiveFactory.getLoss(param.lossFunc), param)
    nodeIndexer.setNodeGradPair(0, rootGradPair)
  }

  private[gbdt] def reset(insSampleSeed: Option[Long] = None,
                          featSampleSeed: Option[Long] = None,
                          classIdOpt: Option[Int] = None): Unit = {
    // 1. get root grad pairs
    var rootGradPair = nodeIndexer.getNodeGradPair(0)
    // 2. reset node indexing & instance sampling
    nodeIndexer.reset()
    if (nodeIndexer.sample(param.regTParam.insSampleRatio, seed = insSampleSeed))
      rootGradPair = insInfo.sumGradPairs(nodeIndexer.nodeToIns,
        nodeIndexer.getNodePosStart(0), nodeIndexer.getNodePosEnd(0),
        param, classIdOpt = classIdOpt)
    nodeIndexer.setNodeGradPair(0, rootGradPair)
    nodeIndexer.setNodeGain(0, rootGradPair.calcGain(param))
    // 3. feature sampling & clear stored histograms
    if (featInfo.sample(param.regTParam.featSampleRatio, seed = featSampleSeed))
      histManager.reset(featInfo.isFeatUsed)
    else
      histManager.removeAll()
  }

  private[gbdt] def updatePreds(tree: GBTTree, learningRate: Float): Unit = {
    require(param.numClass == 2 || !param.multiTree)
    tree.getNodes.foreach {
      case (nid, node) =>
        if (node.isLeaf) {
          if (param.isRegression || param.numClass == 2) {
            val weight = node.getWeight
            insInfo.updatePreds(nid, nodeIndexer, weight, learningRate)
          } else {
            val weights = node.getWeights
            insInfo.updatePreds(nid, nodeIndexer, weights, learningRate)
          }
        }
    }

    if (param.isRegression || param.numClass == 2) {
      for (i <- validData.indices) {
        val weight = tree.predictBinary(validData(i))
        validPreds(i) += weight * learningRate
      }
    } else {
      for (i <- validData.indices) {
        val weights = tree.predictMulti(validData(i))
        for (k <- 0 until param.numClass)
          validPreds(i * param.numClass + k) += weights(k) * learningRate
      }
    }
  }

  private[gbdt] def updatePredsMultiTree(tree: GBTTree, treeId: Int,
                                         learningRate: Float): Unit = {
    require(param.isMultiClassMultiTree)
    tree.getNodes.foreach {
      case (nid, node) =>
        if (node.isLeaf) {
          val weight = node.getWeight
          insInfo.updatePredsMultiTree(nid, treeId, param.numClass, nodeIndexer,
            weight, learningRate)
        }
    }

    for (i <- validData.indices) {
      val weight = tree.predictBinary(validData(i))
      validPreds(i * param.numClass + treeId) += weight * learningRate
    }
  }

  private[gbdt] def evaluate(): Seq[(EvalMetric.Kind, Double, Double)] = {
    ObjectiveFactory.getEvalMetrics(param.evalMetrics).map(evalMetric => {
      val kind = evalMetric.getKind
      val trainMetric = evalMetric.eval(insInfo.predictions, trainLabels)
      val validMetric = evalMetric.eval(validPreds, validLabels)
      (kind, trainMetric, validMetric)
    })
  }
}


private[gbdt] object GBDTWorker {
  @transient private val workers = scala.collection.mutable.Map[Int, GBDTWorker]()

  private[gbdt] def putWorker(worker: GBDTWorker): Unit = {
    workers.synchronized {
      val workerId = worker.workerId
      require(!workers.contains(workerId), s"Worker with ID $workerId already exists")
      workers += workerId -> worker
    }
  }

  @inline private[gbdt] def getWorker[T <: GBDTWorker](workerId: Int): T = {
    workers.get(workerId) match {
      case Some(worker) => worker.asInstanceOf[T]
      case None => throw new RuntimeException(
        s"Cannot fetch worker[$workerId] on executor[${SparkEnv.get.executorId}], " +
          "this may be due to executor lost or memory insufficient")
    }
  }

  @inline private[gbdt] def getArbitrary[T <: GBDTWorker]: T = {
    workers.headOption match {
      case Some(worker) => worker._2.asInstanceOf[T]
      case None => throw new RuntimeException(
        s"Cannot fetch worker on executor[${SparkEnv.get.executorId}], " +
          "this may be due to executor lost or memory insufficient")
    }
  }
}
