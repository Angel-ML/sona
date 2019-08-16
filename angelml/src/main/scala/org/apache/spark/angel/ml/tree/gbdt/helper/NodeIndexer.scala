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

import org.apache.spark.angel.ml.tree.basic.split.SplitEntry
import org.apache.spark.angel.ml.tree.gbdt.dataset.Dataset
import org.apache.spark.angel.ml.tree.util.{ConcurrentUtil, MathUtil, RangeBitSet}
import org.apache.spark.angel.ml.tree.gbdt.histogram.GradPair
import org.apache.spark.angel.ml.tree.gbdt.tree.GBDTParam

import java.{util => ju}

object NodeIndexer {

  private[gbdt] def apply(maxDepth: Int, numIns: Int): NodeIndexer = {
    val maxNodeNum = MathUtil.maxNodeNum(maxDepth)
    val nodePosStart = Array.ofDim[Int](maxNodeNum)
    val nodePosEnd = Array.ofDim[Int](maxNodeNum)
    val nodeActualPosEnd = Array.ofDim[Int](maxNodeNum)
    val nodeToIns = Array.ofDim[Int](numIns)
    val nodeGradPairs = Array.ofDim[GradPair](maxNodeNum)
    val nodeGains = Array.ofDim[Float](maxNodeNum)
    NodeIndexer(nodePosStart, nodePosEnd, nodeActualPosEnd, nodeToIns, nodeGradPairs, nodeGains)
  }
}

case class NodeIndexer(nodePosStart: Array[Int], nodePosEnd: Array[Int], nodeActualPosEnd: Array[Int],
                       nodeToIns: Array[Int], nodeGradPairs: Array[GradPair], nodeGains: Array[Float]) {

  private[gbdt] def reset(): Unit = {
    val numIns = numInstance
    for (nid <- nodeGradPairs.indices) {
      nodeGradPairs(nid) = null
      nodeGains(nid) = Float.NaN
    }
    nodePosStart(0) = 0
    nodePosEnd(0) = numIns
    nodeActualPosEnd(0) = numIns
    for (i <- 0 until numIns) {
      nodeToIns(i) = i
    }
  }

  private[gbdt] def sample(ratio: Float, seed: Option[Long] = None): Boolean = {
    val numIns = numInstance
    val numSample = Math.ceil(numIns * ratio).toInt
    if (numSample < numIns) {
      nodePosEnd(0) = numSample
      MathUtil.shuffle(nodeToIns, seed.getOrElse(
        java.lang.Double.doubleToLongBits(Math.random())
      ))
      // sort to be cache-friendly
      ju.Arrays.sort(nodeToIns, 0, numSample)
      ju.Arrays.sort(nodeToIns, numSample, numIns)
      true
    } else {
      false
    }
  }

  private[gbdt] def getSplitResult(nid: Int, fid: Int, splitEntry: SplitEntry, splits: Array[Float],
                                   dataset: Dataset[Int, Int], param: GBDTParam): RangeBitSet = {
    val res = new RangeBitSet(getNodePosStart(nid), getNodeActualPosEnd(nid))

    def getFlowTo(posId: Int): Int = {
      val insId = nodeToIns(posId)
      val binId = dataset.get(insId, fid)
      if (binId >= 0) splitEntry.flowTo(splits(binId))
      else splitEntry.defaultTo()
    }

    def split(start: Int, end: Int): Unit = {
      require(start <= end)
      if (start == end)
        return

      for (posId <- start until end) {
        val flowTo = getFlowTo(posId)
        if (flowTo == 1) {
          res.set(posId)
        }
      }
    }

    if (ConcurrentUtil.numThread == 1) {
      split(getNodePosStart(nid), getNodeActualPosEnd(nid))
    } else {
      ConcurrentUtil.rangeParallel(split, getNodePosStart(nid),
        getNodeActualPosEnd(nid)).foreach(_.get())
    }
    res
  }

  private[gbdt] def updatePos(nid: Int, splitResult: RangeBitSet): Unit = {
    // in-place update position
    def updateRange(start: Int, end: Int): Int = {
      require(start <= end)
      if (start == end)
        return start

      var left = start
      var right = end - 1
      while (left < right) {
        while (left < right && !splitResult.get(left)) left += 1
        while (left < right && splitResult.get(right)) right -= 1
        if (left < right) {
          val leftInsId = nodeToIns(left)
          val rightInsId = nodeToIns(right)
          nodeToIns(left) = rightInsId
          nodeToIns(right) = leftInsId
          left += 1
          right -= 1
        }
      }
      // find cutting position
      val cut = if (left == right) {
        if (splitResult.get(left)) left
        else left + 1
      } else {
        right
      }
      cut
    }

    // update sampled part
    val cut1 = updateRange(getNodePosStart(nid), getNodePosEnd(nid))
    val numLeftSampled = cut1 - getNodePosStart(nid)
    val numRightSampled = getNodeSize(nid) - numLeftSampled
    // update unsampled part
    val cut2 = updateRange(getNodePosEnd(nid), getNodeActualPosEnd(nid))
    val numLeftUnsampled = cut2 - getNodePosEnd(nid)
    val numRightUnsampled = getNodeActualSize(nid) - getNodeSize(nid) - numLeftUnsampled
    // swap right child's sampled part and left child's unsampled part
    MathUtil.swapRange(nodeToIns, cut1, cut2, getNodePosEnd(nid))
    // set edges
    nodePosStart(2 * nid + 1) = getNodePosStart(nid)
    nodePosEnd(2 * nid + 1) = nodePosStart(2 * nid + 1) + numLeftSampled
    nodeActualPosEnd(2 * nid + 1) = nodePosEnd(2 * nid + 1) + numLeftUnsampled
    nodePosStart(2 * nid + 2) = nodeActualPosEnd(2 * nid + 1)
    nodePosEnd(2 * nid + 2) = nodePosStart(2 * nid + 2) + numRightSampled
    nodeActualPosEnd(2 * nid + 2) = nodePosEnd(2 * nid + 2) + numRightUnsampled
  }

  private[gbdt] def updatePos(nid: Int, dataset: Dataset[Int, Int],
                              splitEntry: SplitEntry, splits: Array[Float]): Unit = {
    val fid = splitEntry.getFid

    def getFlowTo(posId: Int): Int = {
      val insId = nodeToIns(posId)
      val binId = dataset.get(insId, fid)
      if (binId >= 0) {
        splitEntry.flowTo(splits(binId))
      } else {
        splitEntry.defaultTo()
      }
    }

    // in-place update position
    def updateRange(start: Int, end: Int): Int = {
      require(start <= end)
      if (start == end)
        return start

      var left = start
      var right = end - 1
      while (left < right) {
        while (left < right && getFlowTo(left) == 0) left += 1
        while (left < right && getFlowTo(right) == 1) right -= 1
        if (left < right) {
          val leftInsId = nodeToIns(left)
          val rightInsId = nodeToIns(right)
          nodeToIns(left) = rightInsId
          nodeToIns(right) = leftInsId
          left += 1
          right -= 1
        }
      }
      // find cutting position
      val cut = if (left == right) {
        if (getFlowTo(left) == 1) left
        else left + 1
      } else {
        right
      }
      cut
    }

    // update sampled part
    val cut1 = updateRange(getNodePosStart(nid), getNodePosEnd(nid))
    val numLeftSampled = cut1 - getNodePosStart(nid)
    val numRightSampled = getNodeSize(nid) - numLeftSampled
    // update unsampled part
    val cut2 = updateRange(getNodePosEnd(nid), getNodeActualPosEnd(nid))
    val numLeftUnsampled = cut2 - getNodePosEnd(nid)
    val numRightUnsampled = getNodeActualSize(nid) - getNodeSize(nid) - numLeftUnsampled
    // swap right child's sampled part and left child's unsampled part
    MathUtil.swapRange(nodeToIns, cut1, cut2, getNodePosEnd(nid))
    // set edges
    nodePosStart(2 * nid + 1) = getNodePosStart(nid)
    nodePosEnd(2 * nid + 1) = nodePosStart(2 * nid + 1) + numLeftSampled
    nodeActualPosEnd(2 * nid + 1) = nodePosEnd(2 * nid + 1) + numLeftUnsampled
    nodePosStart(2 * nid + 2) = nodeActualPosEnd(2 * nid + 1)
    nodePosEnd(2 * nid + 2) = nodePosStart(2 * nid + 2) + numRightSampled
    nodeActualPosEnd(2 * nid + 2) = nodePosEnd(2 * nid + 2) + numRightUnsampled
  }

  lazy val numInstance: Int = nodeToIns.length

  @inline def nodeCanSplit(nid: Int, param: GBDTParam): Boolean =
    getNodeSize(nid) > param.regTParam.minNodeInstance && getNodeGradPair(nid).satisfyWeight(param)

  @inline def getNodePosStart(nid: Int): Int = nodePosStart(nid)

  @inline def getNodePosEnd(nid: Int) = nodePosEnd(nid)

  @inline def getNodeSize(nid: Int): Int = getNodePosEnd(nid) - getNodePosStart(nid)

  @inline def getNodeActualPosEnd(nid: Int): Int = nodeActualPosEnd(nid)

  @inline def getNodeActualSize(nid: Int): Int = getNodeActualPosEnd(nid) - getNodePosStart(nid)

  @inline def getNodeGradPair(nid: Int): GradPair = nodeGradPairs(nid)

  @inline def setNodeGradPair(nid: Int, nodeGradPair: GradPair): Unit = nodeGradPairs(nid) = nodeGradPair

  @inline def getNodeGain(nid: Int): Float = nodeGains(nid)

  @inline def setNodeGain(nid: Int, nodeGain: Float): Unit = nodeGains(nid) = nodeGain
}
