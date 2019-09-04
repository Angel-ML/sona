package com.tencent.angel.sona.ml.util

import com.tencent.angel.sona.core.DriverContext
import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.sql.{DataFrameReader, SparkSession}

class AngelTestUtils extends SparkFunSuite {
  protected var spark: SparkSession = _
  protected var libsvm: DataFrameReader = _
  protected var dummy: DataFrameReader = _
  protected var sparkConf: SparkConf = _
  protected var driverCtx: DriverContext = _

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession.builder()
      .master("local[2]")
      .appName("AngelClassification")
      .getOrCreate()

    libsvm = spark.read.format("libsvmex")
    dummy = spark.read.format("dummy")
    sparkConf = spark.sparkContext.getConf

    driverCtx = DriverContext.get(sparkConf)
    driverCtx.startAngelAndPSAgent()
  }

  protected override def afterAll(): Unit = {
    super.afterAll()
    driverCtx.stopAngelAndPSAgent()
  }
}
