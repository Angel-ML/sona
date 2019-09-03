package org.apache.spark.sql

import org.apache.spark.sql.types.{ArrayType, NumericType}


object Compatible {
  val numericTypeSimpleString: String = NumericType.simpleString
  val arrayTypeSimpleString: String = ArrayType.simpleString
}


