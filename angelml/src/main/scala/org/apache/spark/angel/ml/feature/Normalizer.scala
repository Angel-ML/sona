/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.angel.ml.feature

import org.apache.spark.angel.ml.linalg.{DenseVector, IntSparseVector, LongSparseVector, VectorUDT, Vectors}
import org.apache.spark.angel.ml.param.{DoubleParam, ParamValidators}
import org.apache.spark.angel.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.angel.ml.{UnaryTransformer, linalg}
import org.apache.spark.annotation.Since
import org.apache.spark.sql.types.DataType

/**
 * Normalize a vector to have unit norm using the given p-norm.
 */
@Since("1.4.0")
class Normalizer @Since("1.4.0") (@Since("1.4.0") override val uid: String)
  extends UnaryTransformer[linalg.Vector, linalg.Vector, Normalizer] with DefaultParamsWritable {

  @Since("1.4.0")
  def this() = this(Identifiable.randomUID("normalizer"))

  /**
   * Normalization in L^p^ space. Must be greater than equal to 1.
   * (default: p = 2)
   * @group param
   */
  @Since("1.4.0")
  val p = new DoubleParam(this, "p", "the p norm value", ParamValidators.gtEq(1))

  setDefault(p -> 2.0)

  /** @group getParam */
  @Since("1.4.0")
  def getP: Double = $(p)

  /** @group setParam */
  @Since("1.4.0")
  def setP(value: Double): this.type = set(p, value)

  override protected def createTransformFunc: linalg.Vector => linalg.Vector = {
    vector => {
      val norm = Vectors.norm(vector, $(p))

      if (norm != 0.0) {
        // For dense vector, we've to allocate new memory for new output vector.
        // However, for sparse vector, the `index` array will not be changed,
        // so we can re-use it to save memory.
        vector match {
          case DenseVector(vs) =>
            val values = vs.clone()
            val size = values.length
            var i = 0
            while (i < size) {
              values(i) /= norm
              i += 1
            }
            Vectors.dense(values)
          case IntSparseVector(size, ids, vs) =>
            val values = vs.clone()
            val nnz = values.length
            var i = 0
            while (i < nnz) {
              values(i) /= norm
              i += 1
            }
            Vectors.sparse(size, ids, values)
          case LongSparseVector(size, ids, vs) =>
            val values = vs.clone()
            val nnz = values.length
            var i = 0
            while (i < nnz) {
              values(i) /= norm
              i += 1
            }
            Vectors.sparse(size, ids, values)
          case v => throw new IllegalArgumentException("Do not support vector type " + v.getClass)
        }
      } else {
        // Since the norm is zero, return the input vector object itself.
        // Note that it's safe since we always assume that the data in RDD
        // should be immutable.
        vector
      }
    }
  }

  override protected def outputDataType: DataType = new VectorUDT()
}

@Since("1.6.0")
object Normalizer extends DefaultParamsReadable[Normalizer] {

  @Since("1.6.0")
  override def load(path: String): Normalizer = super.load(path)
}
