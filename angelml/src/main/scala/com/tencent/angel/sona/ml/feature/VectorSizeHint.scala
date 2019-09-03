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

package com.tencent.angel.sona.ml.feature


import org.apache.spark.SparkException
import com.tencent.angel.sona.ml.Transformer
import com.tencent.angel.sona.ml.attribute.AttributeGroup
import com.tencent.angel.sona.ml.param.{LongParam, Param, ParamMap, ParamValidators}
import com.tencent.angel.sona.ml.param.shared.{HasHandleInvalid, HasInputCol}
import com.tencent.angel.sona.ml.util._
import org.apache.spark.sql.{Column, DataFrame, Dataset}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.StructType
import org.apache.spark.linalg
import org.apache.spark.linalg._

/**
  * :: Experimental ::
  * A feature transformer that adds size information to the metadata of a vector column.
  * VectorAssembler needs size information for its input columns and cannot be used on streaming
  * dataframes without this metadata.
  *
  * Note: VectorSizeHint modifies `inputCol` to include size metadata and does not have an outputCol.
  */
class VectorSizeHint(override val uid: String)
  extends Transformer with HasInputCol with HasHandleInvalid with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("vectSizeHint"))

  /**
    * The size of Vectors in `inputCol`.
    *
    * @group param
    */
  val size: LongParam = new LongParam(
    this,
    "size",
    "Size of vectors in column.",
    { s: Long => s >= 0 })

  /** group getParam */
  def getSize: Long = getOrDefault(size)

  /** @group setParam */
  def setSize(value: Long): this.type = set(size, value)

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /**
    * Param for how to handle invalid entries. Invalid vectors include nulls and vectors with the
    * wrong size. The options are `skip` (filter out rows with invalid vectors), `error` (throw an
    * error) and `optimistic` (do not check the vector size, and keep all rows). `error` by default.
    *
    * Note: Users should take care when setting this param to `optimistic`. The use of the
    * `optimistic` option will prevent the transformer from validating the sizes of vectors in
    * `inputCol`. A mismatch between the metadata of a column and its contents could result in
    * unexpected behaviour or errors when using that column.
    *
    * @group param
    */
  override val handleInvalid: Param[String] = new Param[String](
    this,
    "handleInvalid",
    "How to handle invalid vectors in inputCol. Invalid vectors include nulls and vectors with " +
      "the wrong size. The options are `skip` (filter out rows with invalid vectors), `error` " +
      "(throw an error) and `optimistic` (do not check the vector size, and keep all rows). " +
      "`error` by default.",
    ParamValidators.inArray(VectorSizeHint.supportedHandleInvalids))

  /** @group setParam */
  def setHandleInvalid(value: String): this.type = set(handleInvalid, value)

  setDefault(handleInvalid, VectorSizeHint.ERROR_INVALID)

  override def transform(dataset: Dataset[_]): DataFrame = {
    val localInputCol = getInputCol
    val localSize = getSize
    val localHandleInvalid = getHandleInvalid

    val group = AttributeGroup.fromStructField(dataset.schema(localInputCol))
    val newGroup = validateSchemaAndSize(dataset.schema, group)
    if (localHandleInvalid == VectorSizeHint.OPTIMISTIC_INVALID && group.size == localSize) {
      dataset.toDF()
    } else {
      val newCol: Column = localHandleInvalid match {
        case VectorSizeHint.OPTIMISTIC_INVALID => col(localInputCol)
        case VectorSizeHint.ERROR_INVALID =>
          val checkVectorSizeUDF = udf { vector: linalg.Vector =>
            if (vector == null) {
              throw new SparkException(s"Got null vector in VectorSizeHint, set `handleInvalid` " +
                s"to 'skip' to filter invalid rows.")
            }
            if (vector.size != localSize) {
              throw new SparkException(s"VectorSizeHint Expecting a vector of size $localSize but" +
                s" got ${vector.size}")
            }
            vector
          }
          checkVectorSizeUDF(col(localInputCol))
        case VectorSizeHint.SKIP_INVALID =>
          val checkVectorSizeUDF = udf { vector: linalg.Vector =>
            if (vector != null && vector.size == localSize) {
              vector
            } else {
              null
            }
          }
          checkVectorSizeUDF(col(localInputCol))
      }

      val res = dataset.withColumn(localInputCol, newCol.as(localInputCol, newGroup.toMetadata))
      if (localHandleInvalid == VectorSizeHint.SKIP_INVALID) {
        res.na.drop(Array(localInputCol))
      } else {
        res
      }
    }
  }

  /**
    * Checks that schema can be updated with new size and returns a new attribute group with
    * updated size.
    */
  private def validateSchemaAndSize(schema: StructType, group: AttributeGroup): AttributeGroup = {
    // This will throw a NoSuchElementException if params are not set.
    val localSize = getSize
    val localInputCol = getInputCol

    val inputColType = schema(getInputCol).dataType
    require(
      inputColType.isInstanceOf[VectorUDT],
      s"Input column, $getInputCol must be of Vector type, got $inputColType"
    )
    group.size match {
      case `localSize` => group
      case -1 => new AttributeGroup(localInputCol, localSize)
      case _ =>
        val msg = s"Trying to set size of vectors in `$localInputCol` to $localSize but size " +
          s"already set to ${group.size}."
        throw new IllegalArgumentException(msg)
    }
  }

  override def transformSchema(schema: StructType): StructType = {
    val fieldIndex = schema.fieldIndex(getInputCol)
    val fields = schema.fields.clone()
    val inputField = fields(fieldIndex)
    val group = AttributeGroup.fromStructField(inputField)
    val newGroup = validateSchemaAndSize(schema, group)
    fields(fieldIndex) = inputField.copy(metadata = newGroup.toMetadata)
    StructType(fields)
  }

  override def copy(extra: ParamMap): this.type = defaultCopy(extra)
}

object VectorSizeHint extends DefaultParamsReadable[VectorSizeHint] {

  private[sona] val OPTIMISTIC_INVALID = "optimistic"
  private[sona] val ERROR_INVALID = "error"
  private[sona] val SKIP_INVALID = "skip"
  private[sona] val supportedHandleInvalids: Array[String] =
    Array(OPTIMISTIC_INVALID, ERROR_INVALID, SKIP_INVALID)


  override def load(path: String): VectorSizeHint = super.load(path)
}
