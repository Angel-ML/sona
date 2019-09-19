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

package com.tencent.angel.sona.ml.common
import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.ml.math2.vector._
import org.apache.spark.linalg
import org.apache.spark.linalg.{DenseVector, IntSparseVector, LongSparseVector}
import scala.language.implicitConversions


object MathImplicits {

  type AVector = com.tencent.angel.ml.math2.vector.Vector

  implicit def toAngelVector(bv: linalg.Vector): AVector = {
    bv match {
      case DenseVector(dv: Array[Double]) =>
        VFactory.denseDoubleVector(dv)
      case IntSparseVector(size: Long, idx: Array[Int], values: Array[Double]) =>
        VFactory.sparseDoubleVector(size.toInt, idx, values)
      case LongSparseVector(size: Long, idx: Array[Long], values: Array[Double]) =>
        VFactory.sparseLongKeyDoubleVector(size, idx, values)
    }
  }

  implicit def toSparkVector(bv: AVector): linalg.Vector = {
    bv match {
      case v: IntDoubleVector if v.isDense =>
        new DenseVector(v.getStorage.getValues)
      case v: IntDoubleVector if v.isSparse || v.isSorted =>
        val storage = v.getStorage
        new IntSparseVector(v.dim().toInt, storage.getIndices, storage.getValues)
      case v: LongDoubleVector if v.isSparse || v.isSorted =>
        val storage = v.getStorage
        new LongSparseVector(v.dim(), storage.getIndices, storage.getValues)

      case v: IntFloatVector if v.isDense =>
        new DenseVector(v.getStorage.getValues.map(_.toDouble))
      case v: IntFloatVector if v.isSparse || v.isSorted =>
        val storage = v.getStorage
        new IntSparseVector(v.dim().toInt, storage.getIndices, storage.getValues.map(_.toDouble))
      case v: LongFloatVector if v.isSparse || v.isSorted =>
        val storage = v.getStorage
        new LongSparseVector(v.dim(), storage.getIndices, storage.getValues.map(_.toDouble))

      case v: IntLongVector if v.isDense =>
        new DenseVector(v.getStorage.getValues.map(_.toDouble))
      case v: IntLongVector if v.isSparse || v.isSorted =>
        val storage = v.getStorage
        new IntSparseVector(v.dim().toInt, storage.getIndices, storage.getValues.map(_.toDouble))
      case v: LongLongVector if v.isSparse || v.isSorted =>
        val storage = v.getStorage
        new LongSparseVector(v.dim(), storage.getIndices, storage.getValues.map(_.toDouble))

      case v: IntIntVector if v.isDense =>
        new DenseVector(v.getStorage.getValues.map(_.toDouble))
      case v: IntIntVector if v.isSparse || v.isSorted =>
        val storage = v.getStorage
        new IntSparseVector(v.dim().toInt, storage.getIndices, storage.getValues.map(_.toDouble))
      case v: LongIntVector if v.isSparse || v.isSorted =>
        val storage = v.getStorage
        new LongSparseVector(v.dim(), storage.getIndices, storage.getValues.map(_.toDouble))

      case _ => throw new Exception("Vector Type is not supported!")
    }
  }
}
