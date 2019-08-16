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
package org.apache.spark.angel.ml.tree.gbdt.metadata

import org.apache.spark.angel.ml.tree.util.MathUtil

import java.{util => ju}
import scala.util.Random

object FeatureInfo {
  private[gbdt] val ENUM_THRESHOLD = 16

  private[gbdt] def apply(featTypes: Array[Boolean], numBin: Array[Int],
                          splits: Array[Array[Float]], defaultBins: Array[Int]): FeatureInfo = {

    val isFeatUsed = numBin.map(_ > 0)
    val empCnt = splits.count(_ == null)
    val numCnt = (splits, featTypes).zipped.count(p => p._1 != null && !p._2)
    val catCnt = (splits, featTypes).zipped.count(p => p._1 != null && p._2)
    println(s"Feature info: empty[$empCnt], numerical[$numCnt], categorical[$catCnt]")
    FeatureInfo(featTypes, numBin, splits, defaultBins, isFeatUsed)
  }

  private[gbdt] def apply(featTypes: Array[Boolean], splits: Array[Array[Float]]): FeatureInfo = {
    require(featTypes.length == splits.length)
    val numFeature = featTypes.length
    val numBin = Array.ofDim[Int](numFeature)
    val defaultBins = Array.ofDim[Int](numFeature)
    for (i <- 0 until numFeature) {
      if (splits(i) != null) {
        if (featTypes(i)) {
          numBin(i) = splits(i).length + 1
          defaultBins(i) = splits(i).length
        } else {
          numBin(i) = splits(i).length
          defaultBins(i) = MathUtil.indexOf(splits(i), 0.0f)  // TODO: default bin for continuous feature
        }
      }
    }
    apply(featTypes, numBin, splits, defaultBins)
  }

  private[gbdt] def apply(splits: Array[Array[Float]]): FeatureInfo = {
    apply(splits.map(s => s != null && s.length < ENUM_THRESHOLD), splits)
  }
}

case class FeatureInfo(featTypes: Array[Boolean], numBin: Array[Int], splits: Array[Array[Float]],
                       defaultBins: Array[Int], isFeatUsed: Array[Boolean]) {

  @inline def isCategorical(fid: Int) = featTypes(fid)

  @inline def getNumBin(fid: Int) = numBin(fid)

  @inline def getSplits(fid: Int) = splits(fid)

  @inline def getDefaultBin(fid: Int) = defaultBins(fid)

  def sample(ratio: Float, seed: Option[Long] = None): Boolean = {
    val numFeat = numFeature
    val numSample = Math.ceil(numFeat * ratio).toInt
    if (numSample < numFeat) {
      ju.Arrays.fill(isFeatUsed, false)
      Random.setSeed(seed.getOrElse(
        java.lang.Double.doubleToLongBits(Math.random())
      ))
      for (_ <- 0 until numSample) {
        val randFid = Random.nextInt(numFeat)
        isFeatUsed(randFid) = getNumBin(randFid) > 0
      }
      true
    } else {
      false
    }
  }

  lazy val numFeature: Int = featTypes.length
}