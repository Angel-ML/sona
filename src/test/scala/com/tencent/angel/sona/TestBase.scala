package com.tencent.angel.sona

import java.io.File

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrameReader, SparkSession}
import org.scalatest.FunSuite


trait TestBase extends FunSuite {
  // close log
  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("com.tencent").setLevel(Level.WARN)

  // create SparkSession
  val spark: SparkSession = SparkSession.builder()
    .appName("MLTest")
    .master("local[4]")
    .getOrCreate()

  val libsvmLoader: DataFrameReader = spark.read.format("libsvm")
  val reader: DataFrameReader = spark.read

  private def delete(file: File): Unit = {
    if (file.exists()) {
      if (file.isDirectory) {
        file.listFiles().foreach { f => delete(f) }
        file.delete()
      } else {
        file.delete()
      }
    }
  }

  def check(path: String): String = {
    val f = new File(path)
    if (f.exists()) {
      delete(f)
    }

    f.getAbsolutePath
  }
}
