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
package com.tencent.angel.sona.tree.gbdt.helper

import com.tencent.angel.sona.tree.gbdt.dataset.{Dataset, Partition}
import com.tencent.angel.sona.tree.gbdt.helper.HistManager.NodeHist
import com.tencent.angel.sona.tree.gbdt.histogram.GradPair
import com.tencent.angel.sona.tree.gbdt.metadata.{FeatureInfo, InstanceInfo}
import com.tencent.angel.sona.tree.gbdt.tree.GBDTParam
import com.tencent.angel.sona.tree.util.{ConcurrentUtil, MathUtil}

import java.util.concurrent.Future
import scala.collection.mutable.ArrayBuffer

object HistBuilder {
  private[gbdt] val MIN_INSTANCE_PER_THREAD = 10000
  private[gbdt] val MAX_INSTANCE_PER_THREAD = 1000000

  private[gbdt] def buildScheme(nids: Seq[Int], canSplits: Seq[Boolean], nodeSizes: Seq[Int],
                                param: GBDTParam): (Seq[Int], Seq[Boolean], Seq[Int]) = {
    var cur = 0
    val toBuild = ArrayBuffer[Int]()
    val toSubtract = ArrayBuffer[Boolean]()
    val toRemove = ArrayBuffer[Int]()
    while (cur < nids.length) {
      val nid = nids(cur)
      val sibNid = MathUtil.sibling(nid)
      val parNid = MathUtil.parent(nid)
      if (cur + 1 < nids.length && nids(cur + 1) == sibNid) {
        if (canSplits(cur) || canSplits(cur + 1)) {
          val curSize = nodeSizes(cur)
          val sibSize = nodeSizes(cur + 1)
          if (curSize < sibSize) {
            toBuild += nid
            toSubtract += canSplits(cur + 1)
          } else {
            toBuild += sibNid
            toSubtract += canSplits(cur)
          }
        } else {
          toRemove += parNid
        }
        cur += 2
      } else {
        if (canSplits(cur)) {
          toBuild += nid
          toSubtract += false
        }
        cur += 1
      }
    }
    (toBuild, toSubtract, toRemove)
  }

  // build with consecutive instance ids, for root node without instance sampling
  private def sparseBuild0(param: GBDTParam, partition: Partition[Int, Int],
                           insIdOffset: Int, insInfo: InstanceInfo, start: Int, end: Int,
                           isFeatUsed: Array[Boolean], nodeHist: NodeHist,
                           classIdOpt: Option[Int] = None): Unit = {
    val gradients = insInfo.gradients
    val hessians = insInfo.hessians
    val fids = partition.indices
    val bins = partition.values
    val indexEnds = partition.indexEnds
    if (param.isRegression || param.numClass == 2) {
      var indexStart = if (start == 0) 0 else indexEnds(start - 1)
      for (i <- start until end) {
        val insId = i + insIdOffset
        val grad = gradients(insId)
        val hess = hessians(insId)
        val indexEnd = indexEnds(i)
        for (j <- indexStart until indexEnd) {
          if (isFeatUsed(fids(j))) {
            nodeHist(fids(j)).accumulate(bins(j), grad, hess)
          }
        }
        indexStart = indexEnd
      }
    } else if (param.multiTree) {
      var indexStart = if (start == 0) 0 else indexEnds(start - 1)
      val classId = classIdOpt.get
      for (i <- start until end) {
        val insId = i + insIdOffset
        val grad = gradients(insId * param.numClass + classId)
        val hess = hessians(insId * param.numClass + classId)
        val indexEnd = indexEnds(i)
        for (j <- indexStart until indexEnd) {
          if (isFeatUsed(fids(j))) {
            nodeHist(fids(j)).accumulate(bins(j), grad, hess)
          }
        }
        indexStart = indexEnd
      }
    } else if (!param.fullHessian) {
      var indexStart = if (start == 0) 0 else indexEnds(start - 1)
      for (i <- start until end) {
        val insId = i + insIdOffset
        val indexEnd = indexEnds(i)
        for (j <- indexStart until indexEnd) {
          if (isFeatUsed(fids(j))) {
            nodeHist(fids(j)).accumulate(bins(j),
              gradients, hessians, insId * param.numClass)
          }
        }
        indexStart = indexEnd
      }
    } else {
      var indexStart = if (start == 0) 0 else indexEnds(start - 1)
      for (i <- start until end) {
        val insId = i + insIdOffset
        val gradOffset = insId * param.numClass
        val hessOffset = insId * param.numClass * (param.numClass + 1) / 2
        val indexEnd = indexEnds(i)
        for (j <- indexStart until indexEnd) {
          if (isFeatUsed(fids(j))) {
            nodeHist(fids(j)).accumulate(bins(j),
              gradients, gradOffset, hessians, hessOffset)
          }
        }
        indexStart = indexEnd
      }
    }
  }

  // build given an instance id list, for non-root nodes or root node with instance sampling
  private def sparseBuild1(param: GBDTParam, dataset: Dataset[Int, Int],
                           insInfo: InstanceInfo, insIds: Array[Int], start: Int, end: Int,
                           isFeatUsed: Array[Boolean], nodeHist: NodeHist,
                           classIdOpt: Option[Int] = None): Unit = {
    val gradients = insInfo.gradients
    val hessians = insInfo.hessians
    val insLayouts = dataset.insLayouts
    val partitions = dataset.partitions
    val partOffsets = dataset.partOffsets
    if (param.isRegression || param.numClass == 2) {
      for (i <- start until end) {
        val insId = insIds(i)
        val grad = gradients(insId)
        val hess = hessians(insId)
        val partId = insLayouts(insId)
        val fids = partitions(partId).indices
        val bins = partitions(partId).values
        val partInsId = insId - partOffsets(partId)
        val indexStart = if (partInsId == 0) 0 else partitions(partId).indexEnds(partInsId - 1)
        val indexEnd = partitions(partId).indexEnds(partInsId)
        for (j <- indexStart until indexEnd) {
          if (isFeatUsed(fids(j))) {
            nodeHist(fids(j)).accumulate(bins(j), grad, hess)
          }
        }
      }
    } else if (param.multiTree) {
      val classId = classIdOpt.get
      for (i <- start until end) {
        val insId = insIds(i)
        val grad = gradients(insId * param.numClass + classId)
        val hess = hessians(insId * param.numClass + classId)
        val partId = insLayouts(insId)
        val fids = partitions(partId).indices
        val bins = partitions(partId).values
        val partInsId = insId - partOffsets(partId)
        val indexStart = if (partInsId == 0) 0 else partitions(partId).indexEnds(partInsId - 1)
        val indexEnd = partitions(partId).indexEnds(partInsId)
        for (j <- indexStart until indexEnd) {
          if (isFeatUsed(fids(j))) {
            nodeHist(fids(j)).accumulate(bins(j), grad, hess)
          }
        }
      }
    } else if (!param.fullHessian) {
      for (i <- start until end) {
        val insId = insIds(i)
        val partId = insLayouts(insId)
        val fids = partitions(partId).indices
        val bins = partitions(partId).values
        val partInsId = insId - partOffsets(partId)
        val indexStart = if (partInsId == 0) 0 else partitions(partId).indexEnds(partInsId - 1)
        val indexEnd = partitions(partId).indexEnds(partInsId)
        for (j <- indexStart until indexEnd) {
          if (isFeatUsed(fids(j))) {
            nodeHist(fids(j)).accumulate(bins(j),
              gradients, hessians, insId * param.numClass)
          }
        }
      }
    } else {
      for (i <- start until end) {
        val insId = insIds(i)
        val partId = insLayouts(insId)
        val gradOffset = insId * param.numClass
        val hessOffset = insId * param.numClass * (param.numClass + 1) / 2
        val fids = partitions(partId).indices
        val bins = partitions(partId).values
        val partInsId = insId - partOffsets(partId)
        val indexStart = if (partInsId == 0) 0 else partitions(partId).indexEnds(partInsId - 1)
        val indexEnd = partitions(partId).indexEnds(partInsId)
        for (j <- indexStart until indexEnd) {
          if (isFeatUsed(fids(j))) {
            nodeHist(fids(j)).accumulate(bins(j),
              gradients, gradOffset, hessians, hessOffset)
          }
        }
      }
    }
  }

  private def fillDefaultBins(param: GBDTParam, featInfo: FeatureInfo,
                              sumGradPair: GradPair, nodeHist: NodeHist): Unit = {
    for (fid <- nodeHist.indices) {
      if (nodeHist(fid) != null) {
        val taken = nodeHist(fid).sum()
        val remain = sumGradPair.subtract(taken)
        val defaultBin = featInfo.getDefaultBin(fid)
        nodeHist(fid).accumulate(defaultBin, remain)
      }
    }
  }

  private[gbdt] def buildHistForRoot(param: GBDTParam, dataset: Dataset[Int, Int],
                                     insInfo: InstanceInfo, featInfo: FeatureInfo,
                                     sumGradPair: GradPair, histManager: HistManager,
                                     classIdOpt: Option[Int] = None): Unit = {
    val nodeHist = if (ConcurrentUtil.numThread == 1 || dataset.size < MIN_INSTANCE_PER_THREAD) {
      val threadNodeHist = histManager.getOrAlloc()
      for (partId <- 0 until dataset.numPartition) {
        val partition = dataset.partitions(partId)
        val insIdOffset = dataset.partOffsets(partId)
        sparseBuild0(param, partition, insIdOffset, insInfo, 0, partition.size,
          featInfo.isFeatUsed, threadNodeHist, classIdOpt = classIdOpt)
      }
      threadNodeHist
    } else {
      val histPool = histManager.newFixedHistPool("root", ConcurrentUtil.numThread)
      val futures = ArrayBuffer[Future[Unit]]()
      val batchSize = MIN_INSTANCE_PER_THREAD max (MAX_INSTANCE_PER_THREAD
        min MathUtil.idivCeil(dataset.size, ConcurrentUtil.numThread))
      (0 until dataset.numPartition).foreach(partId => {
        val partition = dataset.partitions(partId)
        val insIdOffset = dataset.partOffsets(partId)
        val thread = (start: Int, end: Int) => {
          val threadNodeHist = histPool.acquire
          sparseBuild0(param, partition, insIdOffset, insInfo, start, end,
            featInfo.isFeatUsed, threadNodeHist, classIdOpt = classIdOpt)
          histPool.release(threadNodeHist)
        }
        futures ++= ConcurrentUtil.rangeParallel(thread, 0, partition.size, batchSize = batchSize)
      })
      futures.foreach(_.get())
      histPool.result
    }
    fillDefaultBins(param, featInfo, sumGradPair, nodeHist)
    histManager.setNodeHist(0, nodeHist)
  }

  private[gbdt] def buildHistForNode(param: GBDTParam, dataset: Dataset[Int, Int],
                                     insInfo: InstanceInfo, featInfo: FeatureInfo,
                                     nid: Int, doSubtract: Boolean,
                                     nodeIndexer: NodeIndexer, histManager: HistManager,
                                     classIdOpt: Option[Int] = None): Unit = {
    val nodeStart = nodeIndexer.getNodePosStart(nid)
    val nodeEnd = nodeIndexer.getNodePosEnd(nid)
    val sumGradPair = nodeIndexer.getNodeGradPair(nid)
    val nodeHist = if (ConcurrentUtil.numThread == 1 || nodeIndexer.getNodeSize(nid) < MIN_INSTANCE_PER_THREAD) {
      val threadNodeHist = histManager.getOrAlloc()
      sparseBuild1(param, dataset, insInfo, nodeIndexer.nodeToIns, nodeStart, nodeEnd,
        featInfo.isFeatUsed, threadNodeHist, classIdOpt = classIdOpt)
      threadNodeHist
    } else {
      val histPool = histManager.newFixedHistPool(s"node[$nid]", ConcurrentUtil.numThread)
      val batchSize = MIN_INSTANCE_PER_THREAD max (MAX_INSTANCE_PER_THREAD
        min MathUtil.idivCeil(nodeIndexer.getNodeSize(nid), ConcurrentUtil.numThread))
      val thread = (start: Int, end: Int) => {
        val threadNodeHist = histPool.acquire
        sparseBuild1(param, dataset, insInfo, nodeIndexer.nodeToIns, start, end,
          featInfo.isFeatUsed, threadNodeHist, classIdOpt = classIdOpt)
        histPool.release(threadNodeHist)
      }
      val futures = ConcurrentUtil.rangeParallel(thread, nodeStart, nodeEnd, batchSize = batchSize)
      futures.foreach(_.get())
      histPool.result
    }
    fillDefaultBins(param, featInfo, sumGradPair, nodeHist)
    histManager.setNodeHist(nid, nodeHist)
    if (doSubtract) {
      val parent = MathUtil.parent(nid)
      val sibling = MathUtil.sibling(nid)
      histSubtract(histManager.getNodeHist(parent), nodeHist)
      histManager.moveNodeHist(parent, sibling)
    } else if (nid > 0) {
      histManager.removeNodeHist(MathUtil.parent(nid))
    }
  }

  private[gbdt] def histSubtract(minuend: NodeHist, subtrahend: NodeHist): Unit = {
    for (fid <- minuend.indices)
      if (minuend(fid) != null)
        minuend(fid).subtractBy(subtrahend(fid))
  }

  private[gbdt] def apply(param: GBDTParam, insInfo: InstanceInfo, featInfo: FeatureInfo): HistBuilder =
    new HistBuilder(param, insInfo, featInfo)

}

import HistBuilder._
class HistBuilder(param: GBDTParam, insInfo: InstanceInfo, featInfo: FeatureInfo) {

  private[gbdt] def buildRootHist(dataset: Dataset[Int, Int], histManager: HistManager,
                                  nodeIndexer: NodeIndexer, classIdOpt: Option[Int] = None): Unit = {
    if (nodeIndexer.getNodeSize(0) == nodeIndexer.numInstance) {
      // build with all instances
      buildHistForRoot(param, dataset, insInfo, featInfo, nodeIndexer.getNodeGradPair(0),
        histManager, classIdOpt = classIdOpt)
    } else {
      // build with sampled data
      buildHistForNode(param, dataset, insInfo, featInfo, 0, false,
        nodeIndexer, histManager, classIdOpt = classIdOpt)
    }
  }

  private[gbdt] def buildHist(toBuild: Seq[Int], toSubtract: Seq[Boolean], dataset: Dataset[Int, Int],
                              histManager: HistManager, nodeIndexer: NodeIndexer,
                              classIdOpt: Option[Int] = None): Unit = {
    (toBuild, toSubtract).zipped.foreach {
      case (nid, doSubtract) =>
        buildHistForNode(param, dataset, insInfo, featInfo, nid, doSubtract,
          nodeIndexer, histManager, classIdOpt = classIdOpt)
    }
  }
}
