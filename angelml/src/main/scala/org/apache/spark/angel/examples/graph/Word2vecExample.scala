package org.apache.spark.angel.examples.graph

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.ps.storage.matrix.PartitionSourceArray
import com.tencent.angel.sona.context.PSContext
import org.apache.spark.angel.graph.utils.{Features, SparkUtils, SubSampling}
import org.apache.spark.angel.graph.word2vec.Word2Vec
import org.apache.spark.angel.ml.param.Param
import org.apache.spark.angel.ml.util.ArgsUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{ArrayType, IntegerType, StructField, StructType}
import org.apache.spark.storage.StorageLevel

import scala.util.Random

object Word2vecExample {

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

    val input = params.getOrElse("input", "")//data path
    val inputColName = params.getOrElse("inputCol", "input")
    val output = params.getOrElse("output", "")
    val embeddingDim = params.getOrElse("embedding", "10").toInt
    val numNegSamples = params.getOrElse("negative", "5").toInt
    val windowSize = params.getOrElse("window", "10").toInt
    val numEpoch = params.getOrElse("epoch", "10").toInt
    val stepSize = params.getOrElse("stepSize", "0.1").toFloat
    val batchSize = params.getOrElse("batchSize", "10000").toInt
    val numPSPartitions = params.getOrElse("numPSParts", "10").toInt
    val numPartitions = params.getOrElse("numParts", "10").toInt
    val withSubSample = params.getOrElse("subSample", "true").toBoolean
    val withRemapping = params.getOrElse("remapping", "true").toBoolean
    val modelType = params.getOrElse("modelType", "cbow")
    val checkpointInterval = params.getOrElse("interval", "10").toInt
    val nodesNumPerRow = params.getOrElse("nodesNumPerRow", "1").toInt

    val numCores = SparkUtils.getNumCores(conf)

    // The number of partition is more than the cores. We do this to achieve dynamic load balance.
    val numDataPartitions = (numCores * 6.25).toInt

    val data = sc.textFile(input)
    data.persist(StorageLevel.DISK_ONLY)

    var corpus: RDD[Array[Int]] = null
    var denseToString: Option[RDD[(Int, String)]] = None

    if (withRemapping) {
      val temp = Features.corpusStringToInt(data)
      corpus = temp._1
      denseToString = Some(temp._2)
    } else {
      corpus = Features.corpusStringToIntWithoutRemapping(data)
    }

    val (maxWordId, docs) = if (withSubSample) {
      corpus.persist(StorageLevel.DISK_ONLY)
      val subsampleTmp = SubSampling.sampling(corpus)
      (subsampleTmp._1, subsampleTmp._2.repartition(numDataPartitions))
    } else {
      val tmp = corpus.repartition(numDataPartitions)
      (tmp.map(_.max).max().toLong + 1, tmp)
    }
    docs.persist(StorageLevel.DISK_ONLY)

    val numDocs = docs.count()
    val numTokens = docs.map(_.length).sum().toLong
    val maxLength = docs.map(_.length).max()
    println(s"numDocs=$numDocs maxWordId=$maxWordId numTokens=$numTokens maxLength=$maxLength")

    val schema = StructType(Array(StructField(inputColName, ArrayType(IntegerType, false))))
    val df = spark.createDataFrame(docs.map{case arr:Array[Int] => Row(arr)}, schema)
    df.show(1)

    corpus.unpersist()
    data.unpersist()


    val word2vec = new Word2Vec()
      .setEmbeddingDim(embeddingDim)
      .setBatchSize(batchSize)
      .setModel(modelType)
      .setNegSample(numNegSamples)
      .setNumRowDataSet(numDocs)
      .setMaxIndex(maxWordId.toInt)
      .setMaxLength(maxLength)
      .setCheckpointInterval(checkpointInterval)
      .setNumEpoch(numEpoch)
      .setNumPSPart(numPSPartitions)
      .setPartitionNum(numPartitions)
      .setStepSize(stepSize)
      .setWindowSize(windowSize)
      .setInput(inputColName)
      .setNodesNumPerRow(nodesNumPerRow)
      .setEmbeddingMatrixName("Word2Vec")

    val model = word2vec.fit(df)

//    val model = new Word2VecModel(param)
//    model.train(docs, param, output + "/embedding")
//    model.save(output + "/embedding", numEpoch)
//    denseToString.map(rdd => rdd.map(f => s"${f._1}:${f._2}").saveAsTextFile(output + "/mapping"))

    PSContext.stop()
    sc.stop()
  }

}