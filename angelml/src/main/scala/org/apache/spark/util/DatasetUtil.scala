package org.apache.spark.util

import org.apache.spark.linalg.{VectorUDT, Vectors}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{ArrayType, DoubleType, FloatType, Metadata}
import org.apache.spark.sql.{Column, DataFrame, Dataset}


object DatasetUtil {
  def withColumns[T](ds: Dataset[T],
                     colNames: Seq[String],
                     cols: Seq[Column],
                     metadata: Seq[Metadata]): DataFrame = {
    require(colNames.size == cols.size,
      s"The size of column names: ${colNames.size} isn't equal to " +
        s"the size of columns: ${cols.size}")
    require(colNames.size == metadata.size,
      s"The size of column names: ${colNames.size} isn't equal to " +
        s"the size of metadata elements: ${metadata.size}")

    val sparkSession = ds.sparkSession
    val queryExecution = ds.queryExecution
    val resolver = sparkSession.sessionState.analyzer.resolver
    val output = queryExecution.analyzed.output

    checkColumnNameDuplication(colNames,
      "in given column names",
      sparkSession.sessionState.conf.caseSensitiveAnalysis)

    val columnMap = colNames.zip(cols).zip(metadata).map { case ((colName: String, col: Column), metadata: Metadata) =>
      colName -> col.as(colName, metadata)
    }.toMap

    val replacedAndExistingColumns = output.map { field =>
      columnMap.find { case (colName, _) =>
        resolver(field.name, colName)
      } match {
        case Some((colName: String, col: Column)) => col.as(colName)
        case _ => new Column(field)
      }
    }

    val newColumns = columnMap.filter { case (colName, col) =>
      !output.exists(f => resolver(f.name, colName))
    }.map { case (colName, col) => col.as(colName) }

    ds.select(replacedAndExistingColumns ++ newColumns: _*)
  }

  def withColumn[T](ds: Dataset[T], colName: String, col: Column, metadata: Metadata): DataFrame = {
    withColumns(ds, Seq(colName), Seq(col), Seq(metadata))
  }

  private def checkColumnNameDuplication(columnNames: Seq[String], colType: String,
                                         caseSensitiveAnalysis: Boolean): Unit = {
    val names = if (caseSensitiveAnalysis) columnNames else columnNames.map(_.toLowerCase)
    if (names.distinct.length != names.length) {
      val duplicateColumns = names.groupBy(identity).collect {
        case (x, ys) if ys.length > 1 => s"`$x`"
      }
      throw new Exception(s"Found duplicate column(s) $colType: ${duplicateColumns.mkString(", ")}")
    }
  }

  /**
    * Cast a column in a Dataset to Vector type.
    *
    * The supported data types of the input column are
    * - Vector
    * - float/double type Array.
    *
    * Note: The returned column does not have Metadata.
    *
    * @param dataset input DataFrame
    * @param colName column name.
    * @return Vector column
    */
  def columnToVector(dataset: Dataset[_], colName: String): Column = {
    val columnDataType = dataset.schema(colName).dataType
    columnDataType match {
      case _: VectorUDT => col(colName)
      case fdt: ArrayType =>
        val transferUDF = fdt.elementType match {
          case _: FloatType => udf(f = (vector: Seq[Float]) => {
            val inputArray = Array.fill[Double](vector.size)(0.0)
            vector.indices.foreach(idx => inputArray(idx) = vector(idx).toDouble)
            Vectors.dense(inputArray)
          })
          case _: DoubleType => udf((vector: Seq[Double]) => {
            Vectors.dense(vector.toArray)
          })
          case other =>
            throw new IllegalArgumentException(s"Array[$other] column cannot be cast to Vector")
        }
        transferUDF(col(colName))
      case other =>
        throw new IllegalArgumentException(s"$other column cannot be cast to Vector")
    }
  }

}
