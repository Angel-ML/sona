package org.apache.spark.angel.graph

import org.apache.spark.angel.graph.line.LINE
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext, SparkFunSuite}

import scala.util.Random


class AngelGraphSuite extends SparkFunSuite {
  private var spark: SparkSession = _
  private var sparkConf: SparkConf = _
  private var sc: SparkContext = _
  private val numPartition: Int = 2
  private val storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY
  private var numEdge: Long = -1
  private var maxNodeId: Long = -1

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession.builder()
      .master(s"local[$numPartition]")
      .appName("AngelGraph")
      .getOrCreate()

    sc = spark.sparkContext
    sparkConf = spark.sparkContext.getConf
  }

  override def afterAll(): Unit = {
    super.afterAll()
    spark.close()
  }

  def readData(input: String): DataFrame = {
    val rdd = sc.textFile(input).mapPartitions { iter =>
      val r = new Random()
      iter.map { line =>
        val arr = line.split(" ")
        val src = arr(0).toInt
        val dst = arr(1).toInt
        (r.nextInt, Row(src, dst))
      }
    }.repartition(numPartition).values.persist(storageLevel)

    numEdge = rdd.count()
    maxNodeId = rdd.map { case Row(src: Int, dst: Int) => math.max(src, dst) }.max().toLong + 1

    val schema = StructType(Array(StructField("src", IntegerType), StructField("dst", IntegerType)))
    val data = spark.createDataFrame(rdd, schema)

    println(s"numEdge=$numEdge maxNodeId=$maxNodeId")

    data
  }

  test("readData") {
    val input = "../data/angel/bc/edge"
    val data = readData(input)
    data.printSchema()
    data.show(10)
  }

  test("line: default params ") {
    val line = new LINE()
    assert(line.getNumEpoch === 10)
    assert(line.getStepSize === 0.00001)
    assert(line.getSrcNodeIdCol === "src")
    assert(line.getDstNodeIdCol === "dst")
  }

  test("line1") {
    val input = "../data/angel/bc/edge"
    val data = readData(input)
    data.printSchema()
    data.show(10)

    val line = new LINE()
      .setStepSize(0.025)
      .setEmbeddingDim(32)
      .setBatchSize(1024)
      .setNumPSPart(2)
      .setNumEpoch(2)
      .setNegSample(5)
      .setOrder(2)
      .setMaxIndex(maxNodeId.toInt)
      .setSrcNodeIdCol("src")
      .setDstNodeIdCol("dst")

    val model = line.fit(data)

    line.write.overwrite().save("trained_models/lineAlgo")

    model.write.overwrite().save("trained_models/lineModels")
  }
}
