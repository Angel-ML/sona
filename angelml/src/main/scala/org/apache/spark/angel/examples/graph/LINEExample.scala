package org.apache.spark.angel.examples.graph

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.ps.storage.matrix.PartitionSourceArray
import com.tencent.angel.sona.context.PSContext
import org.apache.spark.angel.graph.line.LINE
import org.apache.spark.angel.graph.utils.{Features, SparkUtils, SubSampling}
import org.apache.spark.angel.ml.util.ArgsUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.storage.StorageLevel

import scala.util.Random

object LINEExample {
  def main(args: Array[String]): Unit = {
    val params = ArgsUtil.parse(args)

    val spark = SparkSession.builder()
      .master(s"yarn-cluster")
      .appName("LINE")
      .getOrCreate()
    val sc = spark.sparkContext
    val conf = spark.sparkContext.getConf

    conf.set(AngelConf.ANGEL_PS_PARTITION_SOURCE_CLASS, classOf[PartitionSourceArray].getName)
    conf.set(AngelConf.ANGEL_PS_BACKUP_MATRICES, "")

    PSContext.getOrCreate(sc)

    val input = params.getOrElse("input", null)
    val output = params.getOrElse("output", "")
    val embeddingDim = params.getOrElse("embedding", "10").toInt
    val numNegSamples = params.getOrElse("negative", "5").toInt
    val numEpoch = params.getOrElse("epoch", "10").toInt

    val stepSize = params.getOrElse("stepSize", "0.1").toFloat
    val batchSize = params.getOrElse("batchSize", "10000").toInt
    val numPartitions = params.getOrElse("numParts", "10").toInt
    val withSubSample = params.getOrElse("subSample", "true").toBoolean
    val withRemapping = params.getOrElse("remapping", "true").toBoolean
    val order = params.get("order").fold(2)(_.toInt)
    val checkpointInterval = params.getOrElse("interval", "10").toInt


    val numCores = SparkUtils.getNumCores(conf)

    // The number of partition is more than the cores. We do this to achieve dynamic load balance.
    val numDataPartitions = (numCores * 6.25).toInt
    println(s"numDataPartitions=$numDataPartitions")

    val data = sc.textFile(input)
    data.persist(StorageLevel.DISK_ONLY)

    var corpus: RDD[Array[Int]] = null

    if (withRemapping) {
      val temp = Features.corpusStringToInt(data)
      corpus = temp._1
      temp._2.map(f => s"${f._1}:${f._2}").saveAsTextFile(output + "/mapping")
    } else {
      corpus = Features.corpusStringToIntWithoutRemapping(data)
    }

    val(maxNodeId, docs) = if (withSubSample) {
      corpus.persist(StorageLevel.DISK_ONLY)
      val subsampleTmp = SubSampling.sampling(corpus)
      (subsampleTmp._1, subsampleTmp._2.repartition(numDataPartitions))
    } else {
      val tmp = corpus.repartition(numDataPartitions)
      (tmp.map(_.max).max().toLong + 1, tmp)
    }
    val edges = docs.map{
      arr =>
        Row(arr(0), arr(1))
    }

    val schema = StructType(Array(StructField("src", IntegerType), StructField("dst", IntegerType)))
    val df = spark.createDataFrame(edges, schema)

    edges.persist(StorageLevel.DISK_ONLY)

    val numEdge = edges.count()
    println(s"numEdge=$numEdge maxNodeId=$maxNodeId")

    corpus.unpersist()
    data.unpersist()



    val line = new LINE()
      .setStepSize(stepSize)
      .setEmbeddingDim(embeddingDim)
      .setBatchSize(batchSize)
      .setNumPSPart(numPartitions)
      .setNumEpoch(numEpoch)
      .setNegSample(numNegSamples)
      .setMaxIndex(maxNodeId.toInt)
      .setNumRowDataSet(numEdge)
      .setOrder(order)
      .setSrcNodeIdCol("src")
      .setDstNodeIdCol("dst")
//      .setModelCPInterval(checkpointInterval)

    val model = line.fit(df)
    line.write.overwrite().save(output + "/lineAlgo")

//    model.train(edges, param, output + "/embedding")
//    model.save(output + "/embedding", numEpoch)

    PSContext.stop()
    sc.stop()
  }
}
