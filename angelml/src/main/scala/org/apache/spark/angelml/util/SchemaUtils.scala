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

package org.apache.spark.angelml.util

import org.apache.spark.angelml.linalg.VectorUDT
import org.apache.spark.sql.types._


/**
 * Utils for handling schemas.
 */
private[spark] object SchemaUtils {

  // TODO: Move the utility methods to SQL.

  /**
   * Check whether the given schema contains a column of the required data type.
   * @param colName  column name
   * @param dataType  required column data type
   */
  def checkColumnType(
      schema: StructType,
      colName: String,
      dataType: DataType,
      msg: String = ""): Unit = {
    val actualDataType = schema(colName).dataType
    val message = if (msg != null && msg.trim.length > 0) " " + msg else ""
    require(actualDataType.equals(dataType),
      s"Column $colName must be of type ${dataType.catalogString} but was actually " +
        s"${actualDataType.catalogString}.$message")
  }

  /**
   * Check whether the given schema contains a column of one of the require data types.
   * @param colName  column name
   * @param dataTypes  required column data types
   */
  def checkColumnTypes(
      schema: StructType,
      colName: String,
      dataTypes: Seq[DataType],
      msg: String = ""): Unit = {
    val actualDataType = schema(colName).dataType
    val message = if (msg != null && msg.trim.length > 0) " " + msg else ""
    require(dataTypes.exists(actualDataType.equals),
      s"Column $colName must be of type equal to one of the following types: " +
        s"${dataTypes.map(_.catalogString).mkString("[", ", ", "]")} but was actually of type " +
        s"${actualDataType.catalogString}.$message")
  }

  /**
   * Check whether the given schema contains a column of the numeric data type.
   * @param colName  column name
   */
  def checkNumericType(
      schema: StructType,
      colName: String,
      msg: String = ""): Unit = {
    val actualDataType = schema(colName).dataType
    val message = if (msg != null && msg.trim.length > 0) " " + msg else ""
    require(actualDataType.isInstanceOf[NumericType],
      s"Column $colName must be of type ${NumericType.simpleString} but was actually of type " +
      s"${actualDataType.catalogString}.$message")
  }

  /**
   * Appends a new column to the input schema. This fails if the given output column already exists.
   * @param schema input schema
   * @param colName new column name. If this column name is an empty string "", this method returns
   *                the input schema unchanged. This allows users to disable output columns.
   * @param dataType new column data type
   * @return new schema with the input column appended
   */
  def appendColumn(
      schema: StructType,
      colName: String,
      dataType: DataType,
      nullable: Boolean = false): StructType = {
    if (colName.isEmpty) return schema
    appendColumn(schema, StructField(colName, dataType, nullable))
  }

  /**
   * Appends a new column to the input schema. This fails if the given output column already exists.
   * @param schema input schema
   * @param col New column schema
   * @return new schema with the input column appended
   */
  def appendColumn(schema: StructType, col: StructField): StructType = {
    require(!schema.fieldNames.contains(col.name), s"Column ${col.name} already exists.")
    StructType(schema.fields :+ col)
  }

  /**
   * Check whether the given column in the schema is one of the supporting vector type: Vector,
   * Array[Float]. Array[Double]
   * @param schema input schema
   * @param colName column name
   */
  def validateVectorCompatibleColumn(schema: StructType, colName: String): Unit = {
    val typeCandidates = List( new VectorUDT,
      new ArrayType(DoubleType, false),
      new ArrayType(FloatType, false))
    checkColumnTypes(schema, colName, typeCandidates)
  }
}
