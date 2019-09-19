package org.apache.spark.sql.util

import org.apache.spark.linalg.VectorUDT
import org.apache.spark.sql.{AnalysisException, Compatible}
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.types.{ArrayType, DataType, DoubleType, FloatType, NumericType, StructField, StructType}

object SONASchemaUtils {

  /**
   * Checks if an input schema has duplicate column names. This throws an exception if the
   * duplication exists.
   *
   * @param schema schema to check
   * @param colType column type name, used in an exception message
   * @param caseSensitiveAnalysis whether duplication checks should be case sensitive or not
   */
  def checkSchemaColumnNameDuplication(
      schema: StructType, colType: String, caseSensitiveAnalysis: Boolean = false): Unit = {
    checkColumnNameDuplication(schema.map(_.name), colType, caseSensitiveAnalysis)
  }

  // Returns true if a given resolver is case-sensitive
  private def isCaseSensitiveAnalysis(resolver: Resolver): Boolean = {
    if (resolver == caseSensitiveResolution) {
      true
    } else if (resolver == caseInsensitiveResolution) {
      false
    } else {
      sys.error("A resolver to check if two identifiers are equal must be " +
        "`caseSensitiveResolution` or `caseInsensitiveResolution` in o.a.s.sql.catalyst.")
    }
  }

  /**
   * Checks if input column names have duplicate identifiers. This throws an exception if
   * the duplication exists.
   *
   * @param columnNames column names to check
   * @param colType column type name, used in an exception message
   * @param resolver resolver used to determine if two identifiers are equal
   */
  def checkColumnNameDuplication(
      columnNames: Seq[String], colType: String, resolver: Resolver): Unit = {
    checkColumnNameDuplication(columnNames, colType, isCaseSensitiveAnalysis(resolver))
  }

  /**
   * Checks if input column names have duplicate identifiers. This throws an exception if
   * the duplication exists.
   *
   * @param columnNames column names to check
   * @param colType column type name, used in an exception message
   * @param caseSensitiveAnalysis whether duplication checks should be case sensitive or not
   */
  def checkColumnNameDuplication(
      columnNames: Seq[String], colType: String, caseSensitiveAnalysis: Boolean): Unit = {
    val names = if (caseSensitiveAnalysis) columnNames else columnNames.map(_.toLowerCase)
    if (names.distinct.length != names.length) {
      val duplicateColumns = names.groupBy(identity).collect {
        case (x, ys) if ys.length > 1 => s"`$x`"
      }
      throw new AnalysisException(
        s"Found duplicate column(s) $colType: ${duplicateColumns.mkString(", ")}")
    }
  }

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
      s"Column $colName must be of type ${Compatible.numericTypeSimpleString} but was actually of type " +
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