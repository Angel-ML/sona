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
    UDTRegistration.register("org.apache.spark.angelml.linalg.Vector", "org.apache.spark.angelml.linalg.VectorUDT")
    UDTRegistration.register("org.apache.spark.angelml.linalg.DenseVector", "org.apache.spark.angelml.linalg.VectorUDT")
    UDTRegistration.register("org.apache.spark.angelml.linalg.SparseVector", "org.apache.spark.angelml.linalg.VectorUDT")
    UDTRegistration.register("org.apache.spark.angelml.linalg.Matrix", "org.apache.spark.angelml.linalg.MatrixUDT")
    UDTRegistration.register("org.apache.spark.angelml.linalg.DenseMatrix", "org.apache.spark.angelml.linalg.MatrixUDT")
    UDTRegistration.register("org.apache.spark.angelml.linalg.SparseMatrix", "org.apache.spark.angelml.linalg.MatrixUDT")
  }
}
