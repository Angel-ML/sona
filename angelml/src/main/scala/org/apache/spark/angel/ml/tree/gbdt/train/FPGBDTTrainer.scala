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
package org.apache.spark.angel.ml.tree.gbdt.train

import org.apache.spark.SparkContext
import org.apache.spark.angel.ml.tree.basic.split.SplitEntry
import org.apache.spark.angel.ml.tree.gbdt.GBDTModel.GBTTree
import org.apache.spark.angel.ml.tree.gbdt.dataset.Dataset
import org.apache.spark.angel.ml.tree.gbdt.histogram.GradPair
import org.apache.spark.angel.ml.tree.gbdt.metadata.{FeatureInfo, InstanceInfo}
import org.apache.spark.angel.ml.tree.gbdt.tree.{GBDTParam, GBTSplit}
import org.apache.spark.angel.ml.tree.objective.ObjectiveFactory
import org.apache.spark.angel.ml.tree.objective.metric.EvalMetric
import org.apache.spark.angel.ml.tree.util.{ConcurrentUtil, DataLoader, MathUtil, RangeBitSet}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.collection.mutable

object FPGBDTTrainer {

  @inline private def getWorker(workerId: Int): FPGBDTWorker = GBDTWorker.getWorker[FPGBDTWorker](workerId)

  private[train] def featureInfoOfGroup(featureInfo: FeatureInfo, groupId: Int,
                                        groupIdToFid: Array[Int]): FeatureInfo = {
    val groupSize = groupIdToFid.length
    val featTypes = Array.ofDim[Boolean](groupSize)
    val numBin = Array.ofDim[Int](groupSize)
    val splits = Array.ofDim[Array[Float]](groupSize)
    val defaultBins = Array.ofDim[Int](groupSize)
    groupIdToFid.view.zipWithIndex.foreach {
      case (fid, newFid) =>
        featTypes(newFid) = featureInfo.isCategorical(fid)
        numBin(newFid) = featureInfo.getNumBin(fid)
        splits(newFid) = featureInfo.getSplits(fid)
        defaultBins(newFid) = featureInfo.getDefaultBin(fid)
    }
    FeatureInfo(featTypes, numBin, splits, defaultBins)
  }
}

class FPGBDTTrainer(param: GBDTParam) extends GBDTTrainer(param) {
  import FPGBDTTrainer._

  @transient private[gbdt] var bcFidToGroupId: Broadcast[Array[Int]] = _
  @transient private[gbdt] var bcGroupIdToFid: Broadcast[Array[Array[Int]]] = _
  @transient private[gbdt] var bcFidToNewFid: Broadcast[Array[Int]] = _
  @transient private[gbdt] var bcGroupSizes: Broadcast[Array[Int]] = _

  @transient private[gbdt] var workers: RDD[Int] = _

  override protected def initialize(trainInput: String, validInput: String,
                                    numWorker: Int, numThread: Int)
                                   (implicit sc: SparkContext): Unit = {
    println("Start to initialize workers")
    val initStart = System.currentTimeMillis()
    val bcParam = sc.broadcast(param)
    val numFeature = param.regTParam.numFeature
    val numClass = param.numClass
    val numSplit = param.regTParam.numSplit

    // 1. Partition features into groups,
    // get feature id to group id mapping and inverted indexing
    val featGroupStart = System.currentTimeMillis()
    val (fidToGroupId, groupIdToFid) = GBDTTrainer.hashFeatureGrouping(numFeature, numWorker)
    val groupSizes = Array.ofDim[Int](numWorker)
    val fidToNewFid = Array.ofDim[Int](numFeature)
    for (fid <- 0 until numFeature) {
      val groupId = fidToGroupId(fid)
      val newFid = groupSizes(groupId)
      groupSizes(groupId) += 1
      fidToNewFid(fid) = newFid
    }
    val bcFidToGroupId = sc.broadcast(fidToGroupId)
    val bcGroupIdToFid = sc.broadcast(groupIdToFid)
    val bcFidToNewFid = sc.broadcast(fidToNewFid)
    val bcGroupSizes = sc.broadcast(groupSizes)
    println(s"Hash feature grouping cost ${System.currentTimeMillis() - featGroupStart} ms")

    // 2. load data from hdfs
    val loadStart = System.currentTimeMillis()
    val trainSet = Dataset.verticalPartition(
      trainInput, fidToGroupId, fidToNewFid, numWorker
    ).cache()
    val numTrain = trainSet.map(_._1.length).collect().sum
    require(trainSet.map(_._2.size).collect().forall(_ == numTrain))
    println(s"Load $numTrain training data cost " +
      s"${System.currentTimeMillis() - loadStart} ms")

    // 3. collect labels, ensure 0-based indexed and broadcast
    val labelStart = System.currentTimeMillis()
    val labels = new Array[Float](numTrain)
    val partLabels = trainSet.map {
      case (partLabel, dataset) => (dataset.id, partLabel)
    }.collect()
    require(partLabels.map(_._1).distinct.length == partLabels.length)
    var offset = 0
    partLabels.sortBy(_._1).map(_._2).foreach(partLabel => {
      Array.copy(partLabel, 0, labels, offset, partLabel.length)
      offset += partLabel.length
    })
    if (!bcParam.value.isRegression) InstanceInfo.ensureLabel(labels, numClass)
    val bcLabels = sc.broadcast(labels)
    println(s"Collect labels cost ${System.currentTimeMillis() - labelStart} ms")

    // 4. build quantile sketches, get candidate splits,
    // and create feature info, finally broadcast info to all workers
    val getSplitsStart = System.currentTimeMillis()
    val isCategorical = Array.ofDim[Boolean](numFeature)
    val splits = Array.ofDim[Array[Float]](numFeature)
    val groupNNZ = Array.ofDim[Int](numWorker)
    trainSet.map {
      case (_, dataset) =>
        val workerMaxDim = bcGroupSizes.value(dataset.id)
        val sketches = Dataset.createSketches(dataset, workerMaxDim)
        val groupSplits = sketches.map(sketch => {
          GBDTTrainer.getSplits(sketch, numSplit)
        })
        (dataset.id, groupSplits)
    }.collect().foreach {
      case (groupId, groupSplits) =>
        // restore feature id based on column grouping info
        // and set splits to corresponding feature
        val newFidToFid = groupIdToFid(groupId)
        groupSplits.view.zipWithIndex.foreach {
          case ((fIsCategorical, fSplits, nnz), newFid) =>
            val fid = newFidToFid(newFid)
            isCategorical(fid) = fIsCategorical
            splits(fid) = fSplits
            groupNNZ(groupId) += nnz
        }
    }
    val featureInfo = FeatureInfo(isCategorical, splits)
    val bcFeatureInfo = sc.broadcast(featureInfo)
    println(s"Create feature info cost ${System.currentTimeMillis() - getSplitsStart} ms")
    println("Feature info: " + (groupSizes, groupNNZ, 0 until numWorker).zipped.map {
      case (size, nnz, groupId) => s"(group[$groupId] #feature[$size] #nnz[$nnz])"
    }.mkString(" "))

    // 5. initialize workers
    val initWorkerStart = System.currentTimeMillis()
    val validSet = DataLoader.loadLibsvm(validInput, numFeature)
      .repartition(numWorker)
    val workers = trainSet.zipPartitions(validSet, preservesPartitioning = true)(
      (trainIter, validIter) => {
        // training dataset, turn feature values into histogram bin indexes
        val train = trainIter.toArray
        require(train.length == 1)
        val workerId = train.head._2.id
        val featureInfo = featureInfoOfGroup(bcFeatureInfo.value,
          workerId, bcGroupIdToFid.value(workerId))
        val trainData = Dataset.binning(train.head._2, featureInfo)
        val trainLabels = bcLabels.value
        // validation dataset, ensure labels are 0-based indexed
        val valid = validIter.toArray
        val validData = valid.map(_._2)
        val validLabels = valid.map(_._1.toFloat)
        if (!bcParam.value.isRegression) InstanceInfo.ensureLabel(validLabels, numClass)
        // meta data
        val insInfo = InstanceInfo(bcParam.value, trainData.size)
        val featInfo = featureInfo
        // init worker
        val worker = new FPGBDTWorker(workerId, bcParam.value)
        worker.initialize(trainLabels, trainData, validLabels, validData,
          insInfo, featInfo)
        GBDTWorker.putWorker(worker)
        ConcurrentUtil.reset(numThread)
        Iterator(workerId)
      }
    ).cache()
    workers.foreach(workerId =>
      println(s"Worker[$workerId] initialization done"))
    val numValid = workers.map(workerId => {
      val worker = getWorker(workerId)
      require(worker.numTrain == numTrain)
      worker.numValid
    }).collect().sum
    println(s"Load valid data and initialize worker cost " +
      s"${System.currentTimeMillis() - initWorkerStart} ms, " +
      s"$numTrain train data, $numValid valid data")
    trainSet.unpersist()

    this.bcFidToGroupId = bcFidToGroupId
    this.bcGroupIdToFid = bcGroupIdToFid
    this.bcFidToNewFid = bcFidToNewFid
    this.bcGroupSizes = bcGroupSizes
    this.workers = workers
    println(s"Initialization done, cost ${System.currentTimeMillis() - initStart} ms in total")
  }

  override protected def shutdown(): Unit = {
    workers.foreach(_ => ConcurrentUtil.shutdown())
    workers.unpersist()
    bcFidToGroupId.destroy()
    bcGroupIdToFid.destroy()
    bcFidToNewFid.destroy()
    bcGroupSizes.destroy()
  }

  override protected def doCalcGradPairs(): Unit = {
    workers.foreach(workerId => getWorker(workerId).calcGradPairs())
  }

  override protected def doNewTree(classIdOpt: Option[Int] = None): (Int, GradPair) = {
    val seed = Option(java.lang.Double.doubleToLongBits(Math.random()))
    workers.map(workerId => {
      val worker = getWorker(workerId)
      worker.reset(insSampleSeed = seed, classIdOpt = classIdOpt)
      (worker.nodeIndexer.getNodeSize(0),
        worker.nodeIndexer.getNodeGradPair(0))
    }).collect().reduceLeft((r1, r2) => {
      require(r1._1 == r2._1, "Number of instances are different on workers " +
        s"(${r1._1} vs. ${r2._2}), which is incorrect in feature parallel")
      r1
    })
  }

  override protected def doFindRootSplit(rootGradPair: GradPair, classIdOpt: Option[Int] = None): GBTSplit = {
    var res = null.asInstanceOf[GBTSplit]
    workers.map(workerId =>
      (workerId, getWorker(workerId).findRootSplit(classIdOpt = classIdOpt))
    ).collect().foreach {
      case (workerId, split) =>
        if (split != null) {
          if (res == null || res.needReplace(split)) {
            val fidInWorker = split.getSplitEntry.getFid
            val trueFid = bcGroupIdToFid.value(workerId)(fidInWorker)
            split.getSplitEntry.setFid(trueFid)
            res = split
          }
        }
    }
    res
  }

  override protected def doFindSplits(nids: Seq[Int], nodeGradPairs: Seq[GradPair],
                                      canSplits: Seq[Boolean], toBuild: Seq[Int],
                                      toSubtract: Seq[Boolean], toRemove: Seq[Int],
                                      classIdOpt: Option[Int] = None): Map[Int, GBTSplit] = {
    val res = mutable.Map[Int, GBTSplit]()
    workers.map(workerId => {
      (workerId, getWorker(workerId).findSplits(nids, canSplits, toBuild, toSubtract,
        toRemove, classIdOpt = classIdOpt))
    }).collect().foreach {
      case (workerId, splits) =>
        splits.foreach {
          case (nid, split) =>
            if (!res.contains(nid) || res(nid).needReplace(split)) {
              val fidInWorker = split.getSplitEntry.getFid
              val trueFid = bcGroupIdToFid.value(workerId)(fidInWorker)
              split.getSplitEntry.setFid(trueFid)
              res(nid) = split
            }
        }
    }
    res.toMap
  }

  override protected def doSplitNodes(splits: Seq[(Int, GBTSplit)],
                                      classIdOpt: Option[Int] = None): Seq[(Int, Int)] = {
    val fids = splits.map {
      case (_, split) =>
        val fid = split.getSplitEntry.getFid
        val ownerId = bcFidToGroupId.value(fid)
        val fidInWorker = bcFidToNewFid.value(fid)
        (ownerId, fidInWorker)
    }
    val bcSplits = workers.sparkContext.broadcast(splits)
    val bcFids = workers.sparkContext.broadcast(fids)
    val splitResults = workers.flatMap(workerId => {
      getWorker(workerId).getSplitResults(bcSplits.value, bcFids.value).iterator
    }).collect().toMap
    val bcSplitResults = workers.sparkContext.broadcast(splitResults)
    val childrenSizes = workers.map(workerId => {
      getWorker(workerId).splitNodes(bcSplits.value, bcSplitResults.value)
    }).collect().head
    childrenSizes
  }

  override protected def doSetAsLeaves(nids: Seq[Int]): Unit = {
    workers.foreach(workerId => {
      val worker = getWorker(workerId)
      nids.foreach(nid => {
        worker.histManager.removeNodeHist(nid)
        if (nid != 0)
          worker.histManager.removeNodeHist(MathUtil.parent(nid))
      })
    })
  }

  override protected def doFinishTree(tree: GBTTree, learningRate: Float,
                                      classIdOpt: Option[Int] = None): Unit = {
    val bcTree = workers.sparkContext.broadcast(tree)
    if (!param.isMultiClassMultiTree) {
      workers.foreach(workerId => {
        val worker = getWorker(workerId)
        worker.histManager.removeAll()
        worker.updatePreds(bcTree.value, learningRate)
      })
    } else {
      val treeId = classIdOpt.get
      workers.foreach(workerId => {
        val worker = getWorker(workerId)
        worker.histManager.removeAll()
        worker.updatePredsMultiTree(bcTree.value, treeId, learningRate)
      })
    }
  }

  override protected def doEvaluate(): Seq[(EvalMetric.Kind, Double, Double)] = {
    val evalMetrics = param.evalMetrics.map(ObjectiveFactory.getEvalMetricKind)
    val trainMetrics = Array.ofDim[Double](evalMetrics.length)
    val validMetrics = Array.ofDim[Double](evalMetrics.length)
    workers.map(workerId => getWorker(workerId).evaluate())
      .collect().foreach(_.zipWithIndex.foreach {
      case ((kind, train, valid), index) =>
        require(kind == evalMetrics(index))
        trainMetrics(index) += train
        validMetrics(index) += valid
    })
    val numWorker = workers.getNumPartitions
    evalMetrics.indices.foreach(index => {
      trainMetrics(index) /= numWorker
      validMetrics(index) /= numWorker
    })
    (evalMetrics, trainMetrics, validMetrics).zipped.toSeq
  }
}


private[gbdt] class FPGBDTWorker(workerId: Int, param: GBDTParam) extends GBDTWorker(workerId, param) {

  private[gbdt] def findRootSplit(classIdOpt: Option[Int] = None): GBTSplit = {
    histBuilder.buildRootHist(trainData, histManager, nodeIndexer, classIdOpt = classIdOpt)
    splitFinder.findBestSplits(Array(0), nodeIndexer, histManager).headOption match {
      case Some(split) => split._2
      case None => null
    }
  }

  private[gbdt] def findSplits(nids: Seq[Int], canSplits: Seq[Boolean], toBuild: Seq[Int],
                               toSubtract: Seq[Boolean], toRemove: Seq[Int],
                               classIdOpt: Option[Int] = None): Seq[(Int, GBTSplit)] = {
    // 1. remove hist of nodes whose children cannot be split
    toRemove.foreach(nid => histManager.removeNodeHist(nid))
    // 2. build hist for active nodes
    histBuilder.buildHist(toBuild, toSubtract, trainData, histManager,
      nodeIndexer, classIdOpt = classIdOpt)
    // 3. there are nodes which cannot be split, but we build hist for them to perform
    // histogram subtraction, we should remove these hist here
    (nids, canSplits).zipped.foreach {
      case (nid, canSplit) => if (!canSplit) histManager.removeNodeHist(nid)
    }
    // 4. find splits for active nodes
    splitFinder.findBestSplits(nids, nodeIndexer, histManager)
  }

  private[train] def getSplitResults(splits: Seq[(Int, GBTSplit)],
                                     fids: Seq[(Int, Int)]): Seq[(Int, RangeBitSet)] = {
    require(splits.length == fids.length)
    splits.indices.flatMap(i => {
      val (nid, split) = splits(i)
      val (ownerId, fidInWorker) = fids(i)
      if (ownerId == this.workerId)
        Iterator((nid, getSplitResult(nid, fidInWorker, split.getSplitEntry)))
      else
        Iterator.empty
    })
  }

  private[train] def getSplitResult(nid: Int, fidInWorker: Int, splitEntry: SplitEntry): RangeBitSet = {
    require(!splitEntry.isEmpty && splitEntry.getGain > param.regTParam.minSplitGain)
    val splits = featInfo.getSplits(fidInWorker)
    nodeIndexer.getSplitResult(nid, fidInWorker, splitEntry, splits, trainData, param)
  }

  private[train] def splitNodes(splits: Seq[(Int, GBTSplit)],
                                splitResults: Map[Int, RangeBitSet]): Seq[(Int, Int)] = {
    val maxInnerNum = MathUtil.maxInnerNodeNum(param.regTParam.maxDepth)
    splits.flatMap {
      case (nid, split) =>
        val splitResult = splitResults(nid)
        splitNode(nid, splitResult, split.getLeftGradPair, split.getRightGradPair)
        if (2 * nid + 1 >= maxInnerNum)
          histManager.removeNodeHist(nid)
        Iterator((2 * nid + 1, nodeIndexer.getNodeSize(2 * nid + 1)),
          (2 * nid + 2, nodeIndexer.getNodeSize(2 * nid + 2)))
    }
  }

  private[train] def splitNode(nid: Int, splitResult: RangeBitSet, leftGradPair: GradPair,
                               rightGradPair: GradPair): Unit = {
    nodeIndexer.updatePos(nid, splitResult)
    nodeIndexer.setNodeGradPair(2 * nid + 1, leftGradPair)
    nodeIndexer.setNodeGradPair(2 * nid + 2, rightGradPair)
    nodeIndexer.setNodeGain(2 * nid + 1, leftGradPair.calcGain(param))
    nodeIndexer.setNodeGain(2 * nid + 2, rightGradPair.calcGain(param))
  }
}
