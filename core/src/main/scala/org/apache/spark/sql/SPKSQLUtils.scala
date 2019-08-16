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
package org.apache.spark.sql

import org.apache.spark.sql.catalyst.expressions.{GenericRow, GenericRowWithSchema}
import org.apache.spark.sql.types.{StructType, UDTRegistration}

object SPKSQLUtils {
  def append(row: Row, fields: StructType, values: Any*): Row = {
    row match {
      case r: GenericRowWithSchema =>
        val newValues = new Array[Any](r.length + values.length)
        val rLength: Int = r.length
        (0 until rLength).foreach(idx => newValues(idx) = r(idx))
        values.zipWithIndex.foreach { case (value, idx) =>
          newValues(idx + rLength) = value
        }

        val newSchema = if (r.schema != null) {
          val schemaTemp = StructType(r.schema)
          fields.foreach(field => schemaTemp.add(field))
          schemaTemp
        } else {
          null.asInstanceOf[StructType]
        }
        new GenericRowWithSchema(newValues, newSchema)
      case r: GenericRow =>
        val newValues = new Array[Any](r.length + values.length)
        val rLength: Int = r.length
        (0 until rLength).foreach(idx => newValues(idx) = r(idx))
        values.zipWithIndex.foreach { case (value, idx) =>
          newValues(idx + rLength) = value
        }

        new GenericRow(newValues)
      case _ =>
        throw new Exception("Row Error!")
    }
  }

  def registerUDT(): Unit = synchronized{
    UDTRegistration.register("org.apache.spark.angel.ml.linalg.Vector", "org.apache.spark.angel.ml.linalg.VectorUDT")
    UDTRegistration.register("org.apache.spark.angel.ml.linalg.DenseVector", "org.apache.spark.angel.ml.linalg.VectorUDT")
    UDTRegistration.register("org.apache.spark.angel.ml.linalg.SparseVector", "org.apache.spark.angel.ml.linalg.VectorUDT")
    UDTRegistration.register("org.apache.spark.angel.ml.linalg.Matrix", "org.apache.spark.angel.ml.linalg.MatrixUDT")
    UDTRegistration.register("org.apache.spark.angel.ml.linalg.DenseMatrix", "org.apache.spark.angel.ml.linalg.MatrixUDT")
    UDTRegistration.register("org.apache.spark.angel.ml.linalg.SparseMatrix", "org.apache.spark.angel.ml.linalg.MatrixUDT")
  }
}
