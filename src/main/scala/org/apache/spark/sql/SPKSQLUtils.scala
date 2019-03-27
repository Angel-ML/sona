package org.apache.spark.sql

import org.apache.spark.ml.linalg.{VectorUDT, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{GenericRow, GenericRowWithSchema}
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}

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

  def readDummy(spark: SparkSession, path: String, dim: Int): DataFrame = {
    val testRdd: RDD[Row] = spark.sparkContext.textFile(path)
      .map { line =>
        val items = line.trim.split(" +")
        val label = items.head.toDouble
        val feats = Vectors.sparse(dim, items.tail.map(_.toInt), items.tail.map(_ => 1.0))

        Row(label, feats)
      }

    val testSchema = StructType(Array[StructField](
      StructField("label", DoubleType), StructField("features", new VectorUDT())))

    spark.createDataFrame(testRdd, testSchema)
  }
}
