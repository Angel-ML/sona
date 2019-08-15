package org.apache.spark.angel.examples

import com.tencent.angel.ml.core.conf.AngelMLConf
import com.tencent.angel.sona.core.DriverContext
import com.tencent.angel.spark.automl.tuner.TunerParam
import com.tencent.angel.spark.automl.tuner.config.{Configuration, ConfigurationSpace}
import com.tencent.angel.spark.automl.tuner.parameter.{ParamConfig, ParamParser}
import com.tencent.angel.spark.automl.tuner.solver.Solver
import org.apache.spark.angel.ml.automl.TunedParams
import org.apache.spark.angel.ml.classification.{AngelClassifier, AngelClassifierModel}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable
import scala.util.Random

object AutoJsonRunnerExamples {

  def parse(args: Array[String]): Map[String, String] = {
    val cmdArgs = new mutable.HashMap[String, String]()
    println("parsing parameter")
    for (arg <- args) {
      val KEY_VALUE_SEP = ":"
      val sepIdx = arg.indexOf(KEY_VALUE_SEP)
      if (sepIdx != -1) {
        val k = arg.substring(0, sepIdx).trim
        val v = arg.substring(sepIdx + 1).trim
        if (v != "" && v != "Nan" && v != null) {
          cmdArgs.put(k, v)
          println(s"param $k = $v")
        }
      }
    }
    cmdArgs.toMap
  }

  def parseConfig(input: String)(implicit solver: Solver): this.type = {
    val paramConfigs: Array[ParamConfig] = ParamParser.parse(input)
    paramConfigs.foreach{ config =>
      assert(TunedParams.exists(config.getParamName),
        s"${config.getParamName} not supported in tuning")
      addParam(solver, config.getParamName, config.getParamType, config.getValueType,
        config.getParamRange, Random.nextInt())
    }
    this
  }

  def addParam(solver: Solver, pName: String, pType: String, vType: String, config: String, seed: Int = 100): this.type = {
    solver.addParam(pName, pType, vType, config, seed)
    this
  }

  def main(args: Array[String]): Unit = {

    val defaultInput = "hdfs://tl-nn-tdw.tencent-distribute.com:54310/user/tdw_rachelsun/joyjxu/angel-test/daw_data/census_148d_train.libsvm"
    val defaultOutput = "hdfs://tl-nn-tdw.tencent-distribute.com:54310/user/tdw_rachelsun/joyjxu/trained_models"
    val defaultJsonFile = "No json file parsed..."
    val defaultDataFormat = "org.apache.spark.ml.source.libsvm.LibSVMFileFormat"

    val params = parse(args)

    val dataFormat = params.getOrElse("dataFormat", "libsvm")
    val actionType = params.getOrElse("actionType", "train")
    assert(actionType.equalsIgnoreCase("train"), "actionType should be train in hyper-parameter tuning")
    val jsonFile = params.getOrElse("jsonFile", defaultJsonFile)
    val input = params.get("data").get
    val modelPath = params.get("modelPath").get
    val predict = params.get("predictPath").get
    var numBatch = params.getOrElse("numBatch", "10").toInt
    var maxIter = params.getOrElse("maxIter", "2").toInt
    var lr = params.getOrElse("lr", "0.1").toDouble
    val numField = params.getOrElse("numField", "13").toInt

    val tuneIter = params.getOrElse("ml.auto.tuner.iter", "10").toInt
    val minimize = false
    val surrogate = params.getOrElse("ml.auto.tuner.model", "GaussianProcess")
    val config = params.get("ml.auto.tuner.params").get

    // init solver
    val cs: ConfigurationSpace = new ConfigurationSpace("cs")
    implicit val solver: Solver = Solver(cs, minimize, surrogate)
    parseConfig(config)
    TunerParam.setBatchSize(1)

    val spark = SparkSession.builder()
      .master("yarn-cluster")
      .appName("Hyper-parameter tuning of AngelClassification")
      .getOrCreate()

    val sparkConf = spark.sparkContext.getConf
    val driverCtx = DriverContext.get(sparkConf)
    driverCtx.startAngelAndPSAgent()

    val libsvm = spark.read.format("libsvmex")
    val dummy = spark.read.format("dummy")
    var data: DataFrame = null.asInstanceOf[DataFrame]

    if (dataFormat.equalsIgnoreCase("libsvm")) {
      data = libsvm.load(input)
    } else if (dataFormat.equalsIgnoreCase("dummy")){
      data = dummy.load(input)
    } else {
      throw new Exception(s"this dataFormat $dataFormat is not supported!")
    }

    val splitData = data.randomSplit(Array(0.7, 0.3))
    val trainData = splitData(0)
    val testData = splitData(1)

    (0 until tuneIter).foreach { iter =>
      println(s"==========Tuner Iteration[$iter]==========")
      val configs: Array[Configuration] = solver.suggest
      for (config <- configs) {
        val paramMap: mutable.Map[String, Double] = new mutable.HashMap[String, Double]()
        for (paramType <- solver.getParamTypes) {
          paramMap += (paramType._1 -> config.get(paramType._1))
        }
        numBatch = paramMap.getOrElse("numBatch", numBatch.toDouble).toInt
        maxIter = paramMap.getOrElse("maxIter", maxIter.toDouble).toInt
        lr = paramMap.getOrElse("learningRate", lr).toFloat
        val decayAlpha = paramMap.getOrElse("decayAlpha", 0.001)
        val decayBeta = paramMap.getOrElse("decayBeta", 0.001)
        val decayIntervals = paramMap.getOrElse("decayIntervals", 100.toDouble).toInt

        val classifier = new AngelClassifier()
          .setModelJsonFile(jsonFile)
          .setNumClass(2)
          .setNumField(numField)
          .setNumBatch(numBatch)
          .setMaxIter(maxIter)
          .setLearningRate(lr)
          .setDecayAlpha(decayAlpha)
          .setDecayBeta(decayBeta)
          .setDecayIntervals(decayIntervals)

        val model = classifier.fit(trainData)

        val metric = model.evaluate(testData).accuracy
        solver.feed(config, metric)

        if ( (minimize && metric < solver.optimal()._2)
          || (!minimize && metric > solver.optimal()._2)) {
          println(s"find a better configuration = ${config.getVector.toArray.mkString("@")}, " +
            s"metric = $metric")
          model.write.overwrite().save(modelPath)
        }
      }
    }


    driverCtx.stopAngelAndPSAgent()
    spark.close()

  }
}

