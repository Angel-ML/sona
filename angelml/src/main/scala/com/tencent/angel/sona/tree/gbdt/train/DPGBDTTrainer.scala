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

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.SparkContext
import com.tencent.angel.sona.tree.basic.split._
import com.tencent.angel.sona.tree.gbdt.GBDTModel.GBTTree
import com.tencent.angel.sona.tree.gbdt.dataset.{Dataset, IdenticalPartitioner}
import com.tencent.angel.sona.tree.gbdt.helper.HistManager.NodeHist
import com.tencent.angel.sona.tree.gbdt.histogram.{GradPair, Histogram}
import com.tencent.angel.sona.tree.gbdt.metadata.{FeatureInfo, InstanceInfo}
import com.tencent.angel.sona.tree.gbdt.tree.{GBDTParam, GBTSplit}
import com.tencent.angel.sona.tree.objective.ObjectiveFactory
import com.tencent.angel.sona.tree.objective.metric.EvalMetric
import com.tencent.angel.sona.tree.stats.quantile.HeapQuantileSketch
import com.tencent.angel.sona.tree.util.{ConcurrentUtil, DataLoader, MathUtil}
import org.apache.spark.rdd.RDD

import scala.collection.mutable

object DPGBDTTrainer {

  @inline private def getWorker(workerId: Int): DPGBDTWorker = GBDTWorker.getWorker[DPGBDTWorker](workerId)

  @inline private def getArbitrary: DPGBDTWorker = GBDTWorker.getArbitrary[DPGBDTWorker]

  private[train] def mergeHistsAndFindSplits(nodeHistRDD: RDD[Seq[(Int, NodeHist)]],
                                             bcSumGradPairs: Broadcast[Map[Int, GradPair]],
                                             bcFidToGroupId: Broadcast[Array[Int]],
                                             bcGroupIdToFid: Broadcast[Array[Array[Int]]],
                                             bcGroupSizes: Broadcast[Array[Int]]): Map[Int, GBTSplit] = {
    val res = mutable.Map[Int, GBTSplit]()
    val numWorker = nodeHistRDD.getNumPartitions
    nodeHistRDD.flatMap(nodeHists => {
      // change (nid, nodeHist) pairs to (workerId, (nid, subNodeHist)) pairs
      val fidToGroupId = bcFidToGroupId.value
      val groupSizes = bcGroupSizes.value
      nodeHists.flatMap {
        case (nid, nodeHist) =>
          val groups = Array.ofDim[NodeHist](numWorker)
          val curIds = Array.ofDim[Int](numWorker)
          for (workerId <- 0 until numWorker) {
            groups(workerId) = Array.ofDim[Histogram](groupSizes(workerId))
          }
          for (fid <- nodeHist.indices) {
            val targetId = fidToGroupId(fid)
            groups(targetId)(curIds(targetId)) = nodeHist(fid)
            curIds(targetId) += 1
          }
          (0 until numWorker).foreach(workerId =>
            println(s"Group[$workerId] nid[$nid] size[${groups(workerId).length}]")
          )
          (0 until numWorker).map(workerId =>
            (workerId, (nid, groups(workerId)))
          ).iterator
      }
    }).partitionBy(new IdenticalPartitioner(numWorker))
      .mapPartitions(iterator => {
        // now each partition has a portion of features
        if (iterator.isEmpty) {
          Iterator.empty
        } else {
          val (groupIds, nodeHists) = iterator.toArray.unzip
          val groupId = groupIds.head
          require(groupIds.forall(_ == groupId))
          val worker = getArbitrary
          nodeHists.groupBy(_._1).mapValues(seq => {
            val nid = seq.head._1
            require(seq.forall(_._1 == nid))
            val merged = seq.map(_._2).reduceLeft((hist1, hist2) => {
              require(hist1.length == hist2.length)
              for (i <- hist1.indices) {
                if (hist1(i) == null) {
                  hist1(i) = hist2(i)
                } else if (hist2(i) != null) {
                  hist1(i).plusBy(hist2(i))
                }
              }
              hist1
            })
            worker.findSplit(nid, merged, bcSumGradPairs.value(nid),
              bcGroupIdToFid.value(groupId))
          }).filter(_._2 != null).iterator
        }
      }).collect().foreach {
      case (nid, split) =>
        if (!res.contains(nid) || res(nid).needReplace(split))
          res(nid) = split
    }
    res.toMap
  }

  private[train] def getSplitGradPair(histogram: Histogram, splitEntry: SplitEntry,
                                      featInfo: FeatureInfo): (GradPair, GradPair) = {
    val fid = splitEntry.getFid
    val splits = featInfo.getSplits(fid)
    splitEntry.splitType() match {
      case SplitType.SPLIT_POINT =>
        require(!featInfo.isCategorical(fid))
        val splitValue = splitEntry.asInstanceOf[SplitPoint].getFvalue
        var splitId = 0
        while (splitId < splits.length &&
          Math.abs(splits(splitId) - splitValue) > MathUtil.EPSILON)
          splitId += 1
        (histogram.sum(0, splitId), histogram.sum(splitId, histogram.getNumBin))
      case SplitType.SPLIT_SET =>
        require(featInfo.isCategorical(fid))
        val splitSet = splitEntry.asInstanceOf[SplitSet]
        val leftGradPair = histogram.get(featInfo.getDefaultBin(fid))
        if (splitEntry.defaultTo() == 1) leftGradPair.clear()
        for (splitId <- splits.indices) {
          val flowTo = splitSet.flowTo(splits(splitId))
          if (flowTo == 0)
            leftGradPair.plusBy(histogram.get(splitId))
        }
        val rightGradPair = histogram.sum()
        rightGradPair.subtractBy(leftGradPair)
        (leftGradPair, rightGradPair)
      case splitType => throw new RuntimeException(s"Unrecognizable split type: $splitType")
    }
  }
}

class DPGBDTTrainer(param: GBDTParam) extends GBDTTrainer(param) {
  import DPGBDTTrainer._

  @transient private[gbdt] var bcFidToGroupId: Broadcast[Array[Int]] = _
  @transient private[gbdt] var bcGroupIdToFid: Broadcast[Array[Array[Int]]] = _
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
    for (fid <- 0 until numFeature) {
      val groupId = fidToGroupId(fid)
      groupSizes(groupId) += 1
    }
    val bcFidToGroupId = sc.broadcast(fidToGroupId)
    val bcGroupIdToFid = sc.broadcast(groupIdToFid)
    val bcGroupSizes = sc.broadcast(groupSizes)
    println(s"Hash feature grouping cost ${System.currentTimeMillis() - featGroupStart} ms")

    // 2. load data from hdfs
    val loadStart = System.currentTimeMillis()
    val trainSet = Dataset.horizontalPartition(trainInput, numWorker).cache()
    val numTrain = trainSet.map(_._1.length).collect().sum
    require(trainSet.filter(t => t._1.length != t._2.size).isEmpty())
    println(s"Load $numTrain training data cost " +
      s"${System.currentTimeMillis() - loadStart} ms")

    // 3. build quantile sketches, get candidate splits,
    // and create feature info, finally broadcast info to all workers
    val getSplitsStart = System.currentTimeMillis()
    val isCategorical = Array.ofDim[Boolean](numFeature)
    val splits = Array.ofDim[Array[Float]](numFeature)
    val groupNNZ = Array.ofDim[Int](numWorker)
    trainSet.flatMap {
      case (_, dataset) =>
        val sketches = Dataset.createSketches(dataset, numFeature)
        // in order to merge quantile sketches on workers,
        // change the sketches into (workerId, Array[sketch]) format
        val fidToGroupId = bcFidToGroupId.value
        val groups = Array.ofDim[Array[HeapQuantileSketch]](numWorker)
        val curIds = Array.ofDim[Int](numWorker)
        for (workerId <- 0 until numWorker) {
          groups(workerId) = Array.ofDim[HeapQuantileSketch](groupSizes(workerId))
        }
        for (fid <- sketches.indices) {
          val targetId = fidToGroupId(fid)
          val sketch = if (sketches(fid).isEmpty) null else sketches(fid)
          groups(targetId)(curIds(targetId)) = sketch
          curIds(targetId) += 1
        }
        (0 until numWorker).map(workerId =>
          (workerId, groups(workerId))
        ).iterator
    }.partitionBy(new IdenticalPartitioner(numWorker))
      .mapPartitions(iterator => {
        // now each worker gets a portion of features
        // it needs to merge the quantile sketches
        // and gets the candidate splits
        // return (isCategorical, candidate splits, nnz)
        val (workerIds, sketches) = iterator.toArray.unzip
        val workerId = workerIds.head
        require(workerIds.forall(_ == workerId))
        val groupSize = bcGroupSizes.value(workerId)
        require(sketches.forall(_.length == groupSize))
        val groupSplits = (0 until groupSize).map(i => {
          var merged = sketches(0)(i)
          for (srcWorkerId <- 1 until numWorker) {
            val cur = sketches(srcWorkerId)(i)
            if (cur != null) {
              if (merged == null) merged = cur
              else merged.merge(cur)
            }
          }
          GBDTTrainer.getSplits(merged, numSplit)
        }).toArray
        Iterator((workerId, groupSplits))
      }).collect().foreach {
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

    // 4. initialize workers
    val initWorkerStart = System.currentTimeMillis()
    val validSet = DataLoader.loadLibsvm(validInput, numFeature)
      .repartition(numWorker)
    val workers = trainSet.zipPartitions(validSet, preservesPartitioning = true)(
      (trainIter, validIter) => {
        // training dataset, turn feature values into histogram bin indexes
        val train = trainIter.toArray
        require(train.length == 1)
        val workerId = train.head._2.id
        val featureInfo = bcFeatureInfo.value
        val trainData = Dataset.binning(train.head._2, featureInfo)
        val trainLabels = train.head._1
        // validation dataset
        val valid = validIter.toArray
        val validData = valid.map(_._2)
        val validLabels = valid.map(_._1.toFloat)
        // ensure labels are 0-based indexes
        if (!bcParam.value.isRegression) {
          InstanceInfo.ensureLabel(trainLabels, numClass)
          InstanceInfo.ensureLabel(validLabels, numClass)
        }
        // meta data
        val insInfo = InstanceInfo(bcParam.value, trainData.size)
        val featInfo = featureInfo
        // init worker
        val worker = new DPGBDTWorker(workerId, bcParam.value)
        worker.initialize(trainLabels, trainData, validLabels, validData,
          insInfo, featInfo)
        GBDTWorker.putWorker(worker)
        ConcurrentUtil.reset(numThread)
        Iterator(workerId)
      }
    ).cache()
    workers.foreach(workerId =>
      println(s"Worker[$workerId] initialization done"))
    val numValid = workers.map(workerId =>
      getWorker(workerId).numValid
    ).collect().sum
    println(s"Load valid data and initialize worker cost " +
      s"${System.currentTimeMillis() - initWorkerStart} ms, " +
      s"$numTrain train data, $numValid valid data")
    trainSet.unpersist()

    this.bcFidToGroupId = bcFidToGroupId
    this.bcGroupIdToFid = bcGroupIdToFid
    this.bcGroupSizes = bcGroupSizes
    this.workers = workers
    println(s"Initialization done, cost ${System.currentTimeMillis() - initStart} ms in total")
  }

  override protected def shutdown(): Unit = {
    workers.foreach(_ => ConcurrentUtil.shutdown())
    workers.unpersist()
    bcFidToGroupId.destroy()
    bcGroupIdToFid.destroy()
    bcGroupSizes.destroy()
  }

  override protected def doCalcGradPairs(): Unit = {
    workers.foreach(workerId => getWorker(workerId).calcGradPairs())
  }

  override protected def doNewTree(classIdOpt: Option[Int] = None): (Int, GradPair) = {
    val seed = Option(java.lang.Double.doubleToLongBits(Math.random()))
    var rootSize = 0
    var rootGradPair = null.asInstanceOf[GradPair]
    workers.map(workerId => {
      val worker = getWorker(workerId)
      worker.reset(featSampleSeed = seed, classIdOpt = classIdOpt)
      (worker.nodeIndexer.getNodeSize(0),
        worker.nodeIndexer.getNodeGradPair(0))
    }).collect().foreach {
      case (size, gradPair) =>
        rootSize += size
        if (rootGradPair == null)
          rootGradPair = gradPair.copy()
        else
          rootGradPair.plusBy(gradPair)
    }
    (rootSize, rootGradPair)
  }

  override protected def doFindRootSplit(rootGradPair: GradPair, classIdOpt: Option[Int] = None): GBTSplit = {
    val bcRootSumGradPair = workers.sparkContext.broadcast(Map(0 -> rootGradPair))
    val rootHist = workers.map(workerId => {
      val worker = getWorker(workerId)
      worker.buildRootHist(classIdOpt = classIdOpt)
      Seq((0, worker.histManager.getNodeHist(0)))
    })
    mergeHistsAndFindSplits(rootHist, bcRootSumGradPair, bcFidToGroupId,
      bcGroupIdToFid, bcGroupSizes).headOption match {
      case Some(split) => split._2
      case None => null
    }
  }

  override protected def doFindSplits(nids: Seq[Int], nodeGradPairs: Seq[GradPair],
                                      canSplits: Seq[Boolean], toBuild: Seq[Int],
                                      toSubtract: Seq[Boolean], toRemove: Seq[Int],
                                      classIdOpt: Option[Int] = None): Map[Int, GBTSplit] = {
    val bcSumGradPairs = workers.sparkContext.broadcast(
      (nids, nodeGradPairs).zipped.map(
        (nid, nodeSumGradPair) => nid -> nodeSumGradPair).toMap)
    val nodeHists = workers.map(workerId => {
      val worker = getWorker(workerId)
      worker.buildNodeHists(nids, canSplits, toBuild, toSubtract,
        toRemove, classIdOpt = classIdOpt)
      (nids, canSplits).zipped.flatMap {
        case (nid, canSplit) =>
          if (canSplit)
            Iterator((nid, worker.histManager.getNodeHist(nid)))
          else
            Iterator.empty
      }
    })
    mergeHistsAndFindSplits(nodeHists, bcSumGradPairs, bcFidToGroupId,
      bcGroupIdToFid, bcGroupSizes)
  }

  override protected def doSplitNodes(splits: Seq[(Int, GBTSplit)],
                                      classIdOpt: Option[Int] = None): Seq[(Int, Int)] = {
    val bcSplits = workers.sparkContext.broadcast(splits)
    val childrenSizes = mutable.Map[Int, Int]()
    workers.map(workerId => {
      getWorker(workerId).splitNodes(bcSplits.value, classIdOpt = classIdOpt)
    }).collect().foreach(_.foreach {
      case (nid, nodeSize) =>
        if (!childrenSizes.contains(nid))
          childrenSizes(nid) = nodeSize
        else
          childrenSizes(nid) += nodeSize
    })
    childrenSizes.toSeq
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


private[gbdt] class DPGBDTWorker(workerId: Int, param: GBDTParam) extends GBDTWorker(workerId, param) {
  import DPGBDTTrainer._

  private[gbdt] def buildRootHist(classIdOpt: Option[Int] = None): Unit = {
    histBuilder.buildRootHist(trainData, histManager, nodeIndexer, classIdOpt = classIdOpt)
  }

  private[gbdt] def buildNodeHists(nids: Seq[Int], canSplits: Seq[Boolean], toBuild: Seq[Int],
                                   toSubtract: Seq[Boolean], toRemove: Seq[Int],
                                   classIdOpt: Option[Int] = None): Unit = {
    // 1. remove hist of nodes whose children cannot be split
    toRemove.foreach(nid => histManager.removeNodeHist(nid))
    // 2. build hist for active nodes
    histBuilder.buildHist(toBuild, toSubtract, trainData, histManager,
      nodeIndexer, classIdOpt = classIdOpt)
    // 3. there are nodes which cannot be split, but we build hist for them to perform
    // histogram subtraction, we should remove these hist here
    (nids, canSplits).zipped.foreach {
      case (nid, canSplit) =>
        if (!canSplit) histManager.removeNodeHist(nid)
    }
  }

  private[gbdt] def findSplits(nodeHists: Seq[(Int, NodeHist)], sumGradPairs: Map[Int, GradPair],
                               featIds: Seq[Int]): Seq[(Int, GBTSplit)] = {
    nodeHists.flatMap {
      case (nid, nodeHist) =>
        val split = splitFinder.findBestSplit(nodeHist, featIds,
          sumGradPairs(nid), nodeIndexer.getNodeGain(nid))
        if (split.isValid(param.regTParam.minSplitGain))
          Iterator((nid, split))
        else
          Iterator.empty
    }
  }

  private[gbdt] def findSplit(nid: Int, nodeHist: NodeHist, sumGradPair: GradPair,
                              featIds: Seq[Int]): GBTSplit = {
    val split = splitFinder.findBestSplit(nodeHist, featIds,
      sumGradPair, nodeIndexer.getNodeGain(nid))
    if (split.isValid(param.regTParam.minSplitGain))
      split
    else
      null
  }

  private[gbdt] def splitNodes(splits: Seq[(Int, GBTSplit)], classIdOpt: Option[Int] = None): Seq[(Int, Int)] = {
    val maxInnerNum = MathUtil.maxInnerNodeNum(param.regTParam.maxDepth)
    splits.flatMap {
      case (nid, split) =>
        splitNode(nid, split.getSplitEntry, split.getLeftGradPair, split.getRightGradPair,
          classIdOpt = classIdOpt)
        if (2 * nid + 1 >= maxInnerNum)
          histManager.removeNodeHist(nid)
        Iterator((2 * nid + 1, nodeIndexer.getNodeSize(2 * nid + 1)),
          (2 * nid + 2, nodeIndexer.getNodeSize(2 * nid + 2)))
    }
  }

  private[gbdt] def splitNode(nid: Int, splitEntry: SplitEntry,
                              leftSumGradPair: GradPair, rightSumGradPair: GradPair,
                              classIdOpt: Option[Int] = None): Unit = {
    val splits = featInfo.getSplits(splitEntry.getFid)
    nodeIndexer.updatePos(nid, trainData, splitEntry, splits)
    // node grad pairs are used for histogram construction,
    // should be different on workers
    val (leftWorkerGP, rightWorkerGP) = getSplitGradPair(
      histManager.getNodeHist(nid)(splitEntry.getFid),
      splitEntry, featInfo)
    nodeIndexer.setNodeGradPair(2 * nid + 1, leftWorkerGP)
    nodeIndexer.setNodeGradPair(2 * nid + 2, rightWorkerGP)
    // node gains are used for split finding,
    // should be identical among workers
    nodeIndexer.setNodeGain(2 * nid + 1, leftSumGradPair.calcGain(param))
    nodeIndexer.setNodeGain(2 * nid + 2, rightSumGradPair.calcGain(param))
  }
}
