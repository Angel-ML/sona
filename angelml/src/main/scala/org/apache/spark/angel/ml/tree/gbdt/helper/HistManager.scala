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

import org.apache.spark.angel.ml.tree.gbdt.histogram.Histogram
import org.apache.spark.angel.ml.tree.gbdt.metadata.FeatureInfo
import org.apache.spark.angel.ml.tree.gbdt.tree.GBDTParam
import org.apache.spark.angel.ml.tree.util.{ConcurrentUtil, MathUtil}

object HistManager {
  private[gbdt] type NodeHist = Array[Histogram]

  private[gbdt] def apply(param: GBDTParam, featInto: FeatureInfo): HistManager =
    new HistManager(param, featInto)
}

import HistManager._
class HistManager(param: GBDTParam, featInto: FeatureInfo) {
  private[gbdt] var isFeatUsed: Array[Boolean] = _
  private[gbdt] val nodeHists = Array.ofDim[NodeHist](MathUtil.maxInnerNodeNum(param.regTParam.maxDepth))
  private[gbdt] val histStore = Array.ofDim[NodeHist](nodeHists.length + ConcurrentUtil.numThread)
  private[gbdt] var availHist = 0

  private[gbdt] def getOrAlloc(sync: Boolean = false): NodeHist = {
    def doGetOrAlloc(): NodeHist = {
      if (availHist == 0) {
        val numFeat = featInto.numFeature
        val nodeHist = Array.ofDim[Histogram](numFeat)
        for (fid <- 0 until numFeat)
          if (isFeatUsed(fid)) {
            if (!param.isLeafVector) {
              nodeHist(fid) = new Histogram(featInto.getNumBin(fid), 2, false)
            } else {
              nodeHist(fid) = new Histogram(featInto.getNumBin(fid),
                param.numClass, param.fullHessian)
            }
          }
        nodeHist
      } else {
        val nodeHist = histStore(availHist - 1)
        histStore(availHist - 1) = null
        availHist -= 1
        nodeHist
      }
    }

    if (sync)
      this.synchronized(doGetOrAlloc())
    else
      doGetOrAlloc()
  }

  private[gbdt] def free(nodeHist: NodeHist, sync: Boolean = false): Unit = {
    def doFree(): Unit = {
      for (hist <- nodeHist)
        if (hist != null)
          hist.clear()
      histStore(availHist) = nodeHist
      availHist += 1
    }

    if (sync)
      this.synchronized(doFree())
    else
      doFree()
  }

  private[gbdt] def reset(isFeatUsed: Array[Boolean]): Unit = {
    require(isFeatUsed.length == featInto.numFeature)
    this.isFeatUsed = isFeatUsed
    for (i <- this.histStore.indices)
      this.histStore(i) = null
    for (i <- this.nodeHists.indices)
      this.nodeHists(i) = null
    availHist = 0
    System.gc()
  }

  private[gbdt] def getNodeHist(nid: Int): NodeHist = nodeHists(nid)

  private[gbdt] def setNodeHist(nid: Int, nodeHist: NodeHist): Unit = nodeHists(nid) = nodeHist

  private[gbdt] def moveNodeHist(parent: Int, child: Int): Unit = {
    nodeHists(child) = nodeHists(parent)
    nodeHists(parent) = null
  }

  private[gbdt] def removeNodeHist(nid: Int, sync: Boolean = false): Unit = {
    if (nid < nodeHists.length && nodeHists(nid) != null) {
      val nodeHist = nodeHists(nid)
      nodeHists(nid) = null
      free(nodeHist, sync = sync)
    }
  }

  private[gbdt] def removeAll(sync: Boolean = false): Unit = {
    nodeHists.indices.foreach(nid => removeNodeHist(nid, sync = sync))
  }

  private[helper] def newFixedHistPool(name: String, capacity: Int): HistPool = new HistPool {
    private val pool = Array.ofDim[NodeHist](capacity)
    private var numHist = 0
    private var numAcquired = 0

    override private[helper] def acquire: NodeHist = {
      this.synchronized {
        if (numHist == numAcquired) {
          require(numHist < capacity, s"HistPool[$name] Requiring too many node hist, capacity: $capacity")
          pool(numHist) = getOrAlloc(sync = true)
          numHist += 1
        }
        var i = 0
        while (i < numHist && pool(i) == null) i += 1
        val nodeHist = pool(i)
        pool(i) = null
        numAcquired += 1
        nodeHist
      }
    }

    override private[helper] def release(nodeHist: NodeHist): Unit = {
      this.synchronized {
        require(numHist >= numAcquired && numAcquired > 0, s"HistPool[$name] Release before acquire")
        var i = 0
        while (i < numHist && pool(i) != null) i += 1
        pool(i) = nodeHist
        numAcquired -= 1
      }
    }

    override private[helper] def result: NodeHist = {
      this.synchronized {
        require(numHist > 0, s"HistPool[$name] Request result without computation")
        require(numAcquired == 0, s"HistPool[$name] Request result before all requires are released")
        val res = pool.head
        for (i <- 1 until pool.length) {
          val one = pool(i)
          if (one != null) {
            for (fid <- res.indices)
              if (res(fid) != null)
                res(fid).plusBy(one(fid))
            free(one, sync = true)
          }
        }
        res
      }
    }
  }

}

private[helper] sealed trait HistPool {

  private[helper] def acquire: NodeHist

  private[helper] def release(nodeHist: NodeHist): Unit

  private[helper] def result: NodeHist
}
