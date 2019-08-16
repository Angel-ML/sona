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
package org.apache.spark.angel.ml.tree.gbdt.helper

import org.apache.spark.angel.ml.tree.basic.split._
import org.apache.spark.angel.ml.tree.gbdt.helper.HistManager.NodeHist
import org.apache.spark.angel.ml.tree.gbdt.histogram._
import org.apache.spark.angel.ml.tree.gbdt.metadata.FeatureInfo
import org.apache.spark.angel.ml.tree.gbdt.tree.{GBDTParam, GBTSplit}

import scala.collection.mutable.ArrayBuffer

object SplitFinder {
  private[gbdt] def findBestSplitOfOneFeature(param: GBDTParam, featInfo: FeatureInfo, fid: Int, histogram: Histogram,
                                              sumGradPair: GradPair, nodeGain: Float): GBTSplit = {
    if (featInfo.isCategorical(fid)) {
      findBestSplitSet(param, featInfo, fid, histogram, sumGradPair, nodeGain)
    } else {
      findBestSplitPoint(param, featInfo, fid, histogram, sumGradPair, nodeGain)
    }
  }

  private def findBestSplitPoint(param: GBDTParam, featInfo: FeatureInfo, fid: Int, histogram: Histogram,
                                 sumGradPair: GradPair, nodeGain: Float): GBTSplit = {
    val splitPoint = new SplitPoint()
    val leftStat = sumGradPair.copy()
    leftStat.clear()
    val rightStat = sumGradPair.copy()
    var bestLeftStat = null.asInstanceOf[GradPair]
    var bestRightStat = null.asInstanceOf[GradPair]
    val numBin = histogram.getNumBin
    val splits = featInfo.getSplits(fid)
    for (binId <- 0 until numBin - 1) {
      histogram.scan(binId, leftStat, rightStat)
      if (leftStat.satisfyWeight(param) && rightStat.satisfyWeight(param)) {
        val leftGain = leftStat.calcGain(param)
        val rightGain = rightStat.calcGain(param)
        val lossChg = leftGain + rightGain - nodeGain - param.regLambda
        if (splitPoint.needReplace(lossChg)) {
          splitPoint.setFid(fid)
          splitPoint.setFvalue(splits(binId + 1))
          splitPoint.setGain(lossChg)
          bestLeftStat = leftStat.copy()
          bestRightStat = rightStat.copy()
        }
      }
    }
    new GBTSplit(splitPoint, bestLeftStat, bestRightStat)
  }

  private def findBestSplitSet(param: GBDTParam, featInfo: FeatureInfo, fid: Int, histogram: Histogram,
                               sumGradPair: GradPair, nodeGain: Float): GBTSplit = {
    val splits = featInfo.getSplits(fid)
    val defaultBin = featInfo.getDefaultBin(fid)

    def binFlowTo(left: GradPair, bin: GradPair): Int = {
      if (!param.isLeafVector) {
        val sumGrad = sumGradPair.asInstanceOf[BinaryGradPair].getGrad
        val leftGrad = left.asInstanceOf[BinaryGradPair].getGrad
        val binGrad = bin.asInstanceOf[BinaryGradPair].getGrad
        if (binGrad * (2 * leftGrad + binGrad - sumGrad) >= 0.0) 0 else 1
      } else {
        val sumGrad = sumGradPair.asInstanceOf[MultiGradPair].getGrad
        val leftGrad = left.asInstanceOf[MultiGradPair].getGrad
        val binGrad = bin.asInstanceOf[MultiGradPair].getGrad
        var dot = 0.0
        for (i <- 0 until param.numClass)
          dot += binGrad(i) * (2 * leftGrad(i) + binGrad(i) - sumGrad(i))
        if (dot >= 0.0) 0 else 1
      }
    }

    // 1. set default bin to left child
    val leftStat = histogram.get(defaultBin).copy()
    // 2. for other bins, find its location
    var firstFlow = -1
    var curFlow = -1
    var hasRight = false
    var curSplitId = 0
    val edges = ArrayBuffer[Float]()
    edges.sizeHint(FeatureInfo.ENUM_THRESHOLD)
    edges += splits.head
    val binGradPair = leftStat.copy()
    binGradPair.clear()
    val numBin = histogram.getNumBin
    for (binId <- 0 until numBin) {
      if (binId != defaultBin) { // skip default bin
        histogram.get(binId, binGradPair)  // re-use
        val flowTo = binFlowTo(leftStat, binGradPair)
        if (flowTo == 0) leftStat.plusBy(binGradPair)
        else hasRight = true
        if (firstFlow == -1) {
          firstFlow = flowTo
          curFlow = flowTo
        } else if (flowTo != curFlow) {
          edges += splits(curSplitId)
          curFlow = flowTo
        }
        curSplitId += 1
      }
    }
    // 3. create split set
    if (hasRight) {  // whether all bins go the left
      if (edges.length == 1 || edges.last != splits.length)
        edges += splits.last
      val rightStat = sumGradPair.subtract(leftStat)
      if (leftStat.satisfyWeight(param) && rightStat.satisfyWeight(param)) {
        val leftGain = leftStat.calcGain(param)
        val rightGain = rightStat.calcGain(param)
        val lossChg = leftGain + rightGain - nodeGain - param.regLambda
        if (lossChg > 0.0f) {
          val splitSet = new SplitSet(fid, lossChg, edges.toArray, firstFlow, 0)
          return new GBTSplit(splitSet, leftStat, rightStat)
        }
      }
    }
    new GBTSplit()
  }

  private[gbdt] def apply(param: GBDTParam, featInfo: FeatureInfo): SplitFinder =
    new SplitFinder(param, featInfo)

}

import SplitFinder._
class SplitFinder(param: GBDTParam, featInfo: FeatureInfo) {

  private[gbdt] def findBestSplits(nids: Seq[Int], nodeIndexer: NodeIndexer,
                                   histManager: HistManager): Seq[(Int, GBTSplit)] = {
    nids.flatMap(nid => {
      val nodeHist = histManager.getNodeHist(nid)
      if (nodeHist != null) {
        val split = findBestSplit(nodeHist,
          nodeIndexer.getNodeGradPair(nid), nodeIndexer.getNodeGain(nid))
        if (split.isValid(param.regTParam.minSplitGain)) Iterator((nid, split)) else Iterator.empty
      } else {
        Iterator.empty
      }
    })
  }

  private[gbdt] def findBestSplit(nodeHist: NodeHist, sumGradPair: GradPair,
                                  nodeGain: Float): GBTSplit = {
    val best = new GBTSplit()
    for (fid <- nodeHist.indices) {
      if (nodeHist(fid) != null) {
        best.update(findBestSplitOfOneFeature(param, featInfo, fid,
          nodeHist(fid), sumGradPair, nodeGain))
      }
    }
    best
  }

  private[gbdt] def findBestSplit(nodeHist: NodeHist, fids: Seq[Int],
                                  sumGradPair: GradPair, nodeGain: Float): GBTSplit = {
    require(nodeHist.length == fids.length)
    val best = new GBTSplit()
    for (i <- nodeHist.indices) {
      if (nodeHist(i) != null) {
        best.update(findBestSplitOfOneFeature(param, featInfo, fids(i),
          nodeHist(i), sumGradPair, nodeGain))
      }
    }
    best
  }

}
