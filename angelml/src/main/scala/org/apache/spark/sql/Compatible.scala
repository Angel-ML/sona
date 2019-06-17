package org.apache.spark.sql

import org.apache.spark.sql.types.{ArrayType, NumericType}
import org.apache.spark.{SparkConf, SparkContext}


object Compatible  {
  val numericTypeSimpleString = NumericType.simpleString
  val arrayTypeSimpleString = ArrayType.simpleString
  val sparkConf = new SparkConf
  val sc = new SparkContext(sparkConf)
  val ss = new SparkSession(sc)
  val sessionstate = ss.sessionState

}


