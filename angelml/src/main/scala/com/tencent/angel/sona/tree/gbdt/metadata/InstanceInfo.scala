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
package com.tencent.angel.sona.tree.gbdt.metadata

import com.tencent.angel.sona.tree.gbdt.helper.NodeIndexer
import com.tencent.angel.sona.tree.gbdt.histogram.{BinaryGradPair, GradPair, MultiGradPair}
import com.tencent.angel.sona.tree.gbdt.tree.GBDTParam
import com.tencent.angel.sona.tree.objective.loss.{BinaryLoss, Loss, MultiLoss}
import com.tencent.angel.sona.tree.util.ConcurrentUtil

object InstanceInfo {

  private[gbdt] def ensureLabel(labels: Array[Float], numLabel: Int): Unit = {
    if (numLabel == 2) {
      val distinct = labels.distinct.map(_.toInt).sorted
      if (distinct.length < 2) {
        throw new RuntimeException("All labels equal to " + distinct.head)
      } else if (distinct.length > 2) {
        throw new RuntimeException("More than 2 labels are provided: " +
          distinct.mkString(", "))
      } else if (!distinct.contains(0) || !distinct.contains(1)) {
        throw new RuntimeException("Label should be 0 or 1, provided: "
          + distinct.mkString(", "))
      }
    } else {
      var min = Int.MaxValue
      var max = Int.MinValue
      for (label <- labels) {
        val trueLabel = label.toInt
        min = Math.min(min, trueLabel)
        max = Math.max(max, trueLabel)
        if (trueLabel < 0 || trueLabel >= numLabel) {
          throw new RuntimeException("Label should be in " +
            s"[0, ${numLabel - 1}] but got $trueLabel")
        }
      }
    }
  }

  private[gbdt] def ensureGradSize(param: GBDTParam, numIns: Int): (Int, Int) = {
    val numClass = param.numClass
    if (param.isRegression || numClass == 2) {
      (numIns, numIns)
    } else {
      val gradLength = numClass * numIns.toLong
      if (gradLength >= Int.MaxValue)
        throw new RuntimeException("Gradient size exceeds INT_MAX, " +
          s"$numIns(#ins) * $numClass(#class) = $gradLength, " +
          s"please use data parallel or set multi-tree as true")
      val hessLength = if (!param.fullHessian) gradLength else numClass * (numClass + 1) / 2 * numIns.toLong
      if (hessLength >= Int.MaxValue)
        throw new RuntimeException("Hessian size exceeds INT_MAX, " +
          s"$numIns(#ins) * ${numClass * (numClass + 1) / 2}(#class * (#class + 1) / 2) = $hessLength, " +
          s"please use data parallel or set full-hessian as false")
      (gradLength.toInt, hessLength.toInt)
    }
  }

  private[gbdt] def apply(param: GBDTParam, numIns: Int): InstanceInfo = {
    val (gradLength, hessLength) = ensureGradSize(param, numIns)
    val predictions = Array.ofDim[Float](gradLength)
    val gradients = Array.ofDim[Double](gradLength)
    val hessians = Array.ofDim[Double](hessLength)
    InstanceInfo(predictions, gradients, hessians)
  }
}

case class InstanceInfo(predictions: Array[Float], gradients: Array[Double], hessians: Array[Double]) {

  private[gbdt] def calcGradPairs(labels: Array[Float], loss: Loss, param: GBDTParam): GradPair = {
    def calcGP(start: Int, end: Int): GradPair = {
      val numClass = param.numClass
      if (param.isRegression || numClass == 2) {
        // regression or binary classification
        val binaryLoss = loss.asInstanceOf[BinaryLoss]
        var sumGrad = 0.0
        var sumHess = 0.0
        for (insId <- start until end) {
          val grad = binaryLoss.firOrderGrad(predictions(insId), labels(insId))
          val hess = binaryLoss.secOrderGrad(predictions(insId), labels(insId), grad)
          gradients(insId) = grad
          hessians(insId) = hess
          sumGrad += grad
          sumHess += hess
        }
        new BinaryGradPair(sumGrad, sumHess)
      } else if (!param.fullHessian || param.multiTree) { // full-hessian & multi-tree are exclusive
        // multi-label classification, assume hessian matrix is diagonal
        val multiLoss = loss.asInstanceOf[MultiLoss]
        val preds = Array.ofDim[Float](numClass)
        val sumGrad = Array.ofDim[Double](numClass)
        val sumHess = Array.ofDim[Double](numClass)
        for (insId <- start until end) {
          Array.copy(predictions, insId * numClass, preds, 0, numClass)
          val grad = multiLoss.firOrderGrad(preds, labels(insId))
          val hess = multiLoss.secOrderGradDiag(preds, labels(insId), grad)
          for (k <- 0 until numClass) {
            gradients(insId * numClass + k) = grad(k)
            hessians(insId * numClass + k) = hess(k)
            sumGrad(k) += grad(k)
            sumHess(k) += hess(k)
          }
        }
        new MultiGradPair(sumGrad, sumHess)
      } else {
        // multi-label classification, represent hessian matrix as lower triangular matrix
        val multiLoss = loss.asInstanceOf[MultiLoss]
        val preds = Array.ofDim[Float](numClass)
        val sumGrad = Array.ofDim[Double](numClass)
        val sumHess = Array.ofDim[Double](numClass * (numClass + 1) / 2)
        for (insId <- start until end) {
          Array.copy(predictions, insId * numClass, preds, 0, numClass)
          val grad = multiLoss.firOrderGrad(preds, labels(insId))
          val hess = multiLoss.secOrderGradFull(preds, labels(insId), grad)
          val gradOffset = insId * numClass
          val hessOffset = insId * numClass * (numClass + 1) / 2
          for (k <- 0 until numClass) {
            gradients(gradOffset + k) = grad(k)
            sumGrad(k) += grad(k)
          }
          for (k <- 0 until numClass * (numClass + 1) / 2) {
            hessians(hessOffset + k) = hess(k)
            sumHess(k) += hess(k)
          }
        }
        new MultiGradPair(sumGrad, sumHess)
      }
    }

    val numIns = labels.length
    if (ConcurrentUtil.threadPool == null) {
      calcGP(0, numIns)
    } else {
      ConcurrentUtil.rangeParallel(calcGP, 0, numIns)
        .map(_.get())
        .reduceLeft((gp1, gp2) => { gp1.plusBy(gp2); gp1 })
    }
  }

  private[gbdt] def sumGradPairs(insIds: Array[Int], from: Int, to: Int,
                                 param: GBDTParam, classIdOpt: Option[Int] = None): GradPair = {
    if (from == to) {
      if (!param.isLeafVector)
        return new BinaryGradPair()
      else
        return new MultiGradPair(param.numClass, param.fullHessian)
    }

    def sumGP(start: Int, end: Int): GradPair = {
      val numClass = param.numClass
      if (param.isRegression || numClass == 2) {
        // regression task or binary classification
        var sumGrad = 0.0
        var sumHess = 0.0
        for (i <- start until end) {
          val insId = insIds(i)
          sumGrad += gradients(insId)
          sumHess += hessians(insId)
        }
        new BinaryGradPair(sumGrad, sumHess)
      } else if (param.multiTree) {
        // multi-label classification, use one-vs-rest trees
        val classId = classIdOpt.get
        var sumGrad = 0.0
        var sumHess = 0.0
        for (i <- start until end) {
          val insId = insIds(i)
          sumGrad += gradients(insId * numClass + classId)
          sumHess += hessians(insId * numClass + classId)
        }
        new BinaryGradPair(sumGrad, sumHess)
      } else if (!param.fullHessian) {
        // multi-label classification, use multi-label tree, assume hessian matrix is diagonal
        val sumGrad = Array.ofDim[Double](numClass)
        val sumHess = Array.ofDim[Double](numClass)
        for (i <- start until end) {
          val insId = insIds(i)
          for (k <- 0 until numClass) {
            sumGrad(k) += gradients(insId * numClass + k)
            sumHess(k) += hessians(insId * numClass + k)
          }
        }
        new MultiGradPair(sumGrad, sumHess)
      } else {
        // multi-label classification, use multi-label tree, represent hessian matrix as lower triangular matrix
        val sumGrad = Array.ofDim[Double](numClass)
        val sumHess = Array.ofDim[Double](numClass * (numClass + 1) / 2)
        for (i <- start until end) {
          val insId = insIds(i)
          val gradOffset = insId * numClass
          val hessOffset = insId * numClass * (numClass + 1) / 2
          for (k <- 0 until numClass)
            sumGrad(k) += gradients(gradOffset + k)
          for (k <- 0 until numClass * (numClass + 1) / 2)
            sumHess(k) += hessians(hessOffset + k)
        }
        new MultiGradPair(sumGrad, sumHess)
      }
    }

    if (ConcurrentUtil.threadPool == null) {
      sumGP(from, to)
    } else {
      ConcurrentUtil.rangeParallel(sumGP, from, to)
        .map(_.get())
        .reduceLeft((gp1, gp2) => { gp1.plusBy(gp2); gp1 })
    }
  }

  private[gbdt] def updatePreds(nid: Int, nodeIndexer: NodeIndexer, update: Float, learningRate: Float): Unit = {
    val update_ = update * learningRate
    val nodeStart = nodeIndexer.getNodePosStart(nid)
    val nodeEnd = nodeIndexer.getNodeActualPosEnd(nid)
    val nodeToIns = nodeIndexer.nodeToIns
    for (posId <- nodeStart until nodeEnd) {
      val insId = nodeToIns(posId)
      predictions(insId) += update_
    }
  }

  private[gbdt] def updatePreds(nid: Int, nodeIndexer: NodeIndexer, update: Array[Float], learningRate: Float): Unit = {
    val numClass = update.length
    val update_ = update.map(_ * learningRate)
    val nodeStart = nodeIndexer.getNodePosStart(nid)
    val nodeEnd = nodeIndexer.getNodeActualPosEnd(nid)
    val nodeToIns = nodeIndexer.nodeToIns
    for (posId <- nodeStart until nodeEnd) {
      val insId = nodeToIns(posId)
      val offset = insId * numClass
      for (k <- 0 until numClass)
        predictions(offset + k) += update_(k)
    }
  }

  private[gbdt] def updatePredsMultiTree(nid: Int, treeId: Int, numClass: Int, nodeIndexer: NodeIndexer,
                                         update: Float, learningRate: Float): Unit = {
    val update_ = update * learningRate
    val nodeStart = nodeIndexer.getNodePosStart(nid)
    val nodeEnd = nodeIndexer.getNodeActualPosEnd(nid)
    val nodeToIns = nodeIndexer.nodeToIns
    for (posId <- nodeStart until nodeEnd) {
      val insId = nodeToIns(posId)
      predictions(insId * numClass + treeId) += update_
    }
  }
}
