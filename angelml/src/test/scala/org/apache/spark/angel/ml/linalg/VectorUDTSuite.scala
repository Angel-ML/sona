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

package org.apache.spark.angel.ml.linalg

import org.apache.spark.SparkFunSuite
import org.apache.spark.angel.ml.feature.LabeledPoint
import org.apache.spark.sql.catalyst.JavaTypeInference
import org.apache.spark.sql.types._

class VectorUDTSuite extends SparkFunSuite {

  override def beforeAll(): Unit = {
    super.beforeAll()
    UDTRegistration.register("org.apache.spark.angel.ml.linalg.Vector", "org.apache.spark.angel.ml.linalg.VectorUDT")
    UDTRegistration.register("org.apache.spark.angel.ml.linalg.DenseVector", "org.apache.spark.angel.ml.linalg.VectorUDT")
    UDTRegistration.register("org.apache.spark.angel.ml.linalg.SparseVector", "org.apache.spark.angel.ml.linalg.VectorUDT")
    UDTRegistration.register("org.apache.spark.angel.ml.linalg.Matrix", "org.apache.spark.angel.ml.linalg.MatrixUDT")
    UDTRegistration.register("org.apache.spark.angel.ml.linalg.DenseMatrix", "org.apache.spark.angel.ml.linalg.MatrixUDT")
    UDTRegistration.register("org.apache.spark.angel.ml.linalg.SparseMatrix", "org.apache.spark.angel.ml.linalg.MatrixUDT")
    UDTRegistration.register("org.apache.spark.angel.ml.linalg.IntSparseVector", "org.apache.spark.angel.ml.linalg.VectorUDT")
    UDTRegistration.register("org.apache.spark.angel.ml.linalg.LongSparseVector", "org.apache.spark.angel.ml.linalg.VectorUDT")

  }

  test("preloaded VectorUDT") {
    val dv1 = Vectors.dense(Array.empty[Double])
    val dv2 = Vectors.dense(1.0, 2.0)
    val sv1 = Vectors.sparse(2, Array.empty[Int], Array.empty)
    val sv2 = Vectors.sparse(2, Array(1L), Array(2.0))

    for (v <- Seq(dv1, dv2, sv1, sv2)) {
      val udt = UDTRegistration.getUDTFor(v.getClass.getName).get.newInstance()
        .asInstanceOf[VectorUDT]
      assert(v === udt.deserialize(udt.serialize(v)))
      assert(udt.typeName == "vector")
      assert(udt.simpleString == "vector")
    }
  }

  test("JavaTypeInference with VectorUDT") {
    val (dataType, _) = JavaTypeInference.inferDataType(classOf[LabeledPoint])
    assert(dataType.asInstanceOf[StructType].fields.map(_.dataType)
      === Seq(new VectorUDT, DoubleType))
  }
}
