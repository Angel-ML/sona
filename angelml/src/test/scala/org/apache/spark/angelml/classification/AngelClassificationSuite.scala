package org.apache.spark.angelml.classification

import com.tencent.angel.sona.core.DriverContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrameReader, SparkSession}
import org.scalatest.FunSuite


class AngelClassificationSuite extends FunSuite {

  val spark: SparkSession = SparkSession.builder()
    .master("local[2]")
    .appName("AngelClassification")
    .getOrCreate()

  val libsvm: DataFrameReader = spark.read.format("libsvm")
  val sparkConf: SparkConf = spark.sparkContext.getConf

  test("train") {
    val driverCtx = DriverContext.get(sparkConf)
    driverCtx.startAngelAndPSAgent()
    val trainData = libsvm.load("./data/angel/census/census_148d_train.libsvm")

    val classifier = new AngelClassifier()
      .setModelJsonFile("./angelml/src/test/jsons/daw.json")
      .setNumClass(2)
      .setNumBatch(10)
      .setMaxIter(10)
      .setLearningRate(0.1)
      .setNumField(13)

    val model = classifier.fit(trainData)


    model.write.overwrite().save("test2")

    driverCtx.stopAngelAndPSAgent()
  }
}
