package com.tencent.angel.sona.regresser

import com.tencent.angel.client.AngelPSClient
import com.tencent.angel.ml.core.PSOptimizerProvider
import com.tencent.angel.ml.core.conf.MLCoreConf
import org.apache.spark.internal.Logging
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util._
import org.apache.spark.ml.{APredictorParams, ARegressionModel}
import org.apache.spark.rdd.RDD
import com.tencent.angel.sona.common.measure.RegressionSummary
import com.tencent.angel.sona.common.measure.evaluating.RegressionSummaryImpl
import com.tencent.angel.sona.common.{AngelSaverLoader, AngelSparkModel, Predictor}
import com.tencent.angel.sona.core._
import org.apache.spark.sql.types.{DoubleType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}


class AngelRegressorModel(override val uid: String, override val angelModelName: String)
  extends ARegressionModel[Vector, AngelRegressorModel] with AngelSparkModel
    with APredictorParams with MLWritable with Logging {
  @transient implicit override val psClient: AngelPSClient = DriverContext.get().getAngelClient
  override val numFeatures: Int = sharedConf.getInt(MLCoreConf.ML_FEATURE_INDEX_RANGE, -1)

  def findSummaryModel(): (AngelRegressorModel, String) = {
    val model = if ($(predictionCol).isEmpty) {
      copy(ParamMap.empty).setPredictionCol("prediction_" + java.util.UUID.randomUUID.toString)
    } else {
      this
    }
    (model, model.getPredictionCol)
  }

  override def updateFromProgramSetting(): this.type = {
    sharedConf.set(MLCoreConf.ML_IS_DATA_SPARSE, getIsSparse.toString)
    sharedConf.set(MLCoreConf.ML_MODEL_TYPE, getModelType)
    sharedConf.set(MLCoreConf.ML_FIELD_NUM, getNumField.toString)

    sharedConf.set(MLCoreConf.ML_FEATURE_INDEX_RANGE, getNumFeature.toString)
    sharedConf.set(MLCoreConf.ML_OPTIMIZER_JSON_PROVIDER, classOf[PSOptimizerProvider].getName)

    this
  }

  def evaluate(dataset: Dataset[_]): RegressionSummary = {
    val taskNum = dataset.rdd.getNumPartitions
    setNumTask(taskNum)

    // Handle possible missing or invalid prediction columns
    val (summaryModel, predictionColName) = findSummaryModel()
    new RegressionSummaryImpl(summaryModel.transform(dataset),
      predictionColName, $(labelCol))
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema, logging = true)

    val taskNum = dataset.rdd.getNumPartitions
    setNumTask(taskNum)

    val featIdx: Int = dataset.schema.fieldIndex($(featuresCol))

    val predictionColName = if ($(predictionCol).isEmpty) {
      val value = s"prediction_${java.util.UUID.randomUUID.toString}"
      set(predictionCol, value)
      value
    } else {
      $(predictionCol)
    }

    if (bcValue == null) {
      finalizeConf(psClient)
      bcValue = dataset.rdd.context.broadcast(ExecutorContext(sharedConf, taskNum, null))
      DriverContext.get().registerBroadcastVariables(bcValue)
    }

    val predictor = new Predictor(bcValue, featIdx, predictionColName)

    val newSchema: StructType = dataset.schema.add(predictionColName, DoubleType)

    val rddRow = dataset.rdd.asInstanceOf[RDD[Row]]
    val rddWithPredicted = rddRow.mapPartitions(predictor.predictRDD, preservesPartitioning = true)
    dataset.sparkSession.createDataFrame(rddWithPredicted, newSchema)
  }

  override def write: MLWriter = new AngelSaverLoader.AngelModelWriter(this)

  override def copy(extra: ParamMap): AngelRegressorModel = defaultCopy(extra)

  override protected def predict(features: Vector): Double = ???
}

object AngelRegressorModel extends MLReadable[AngelRegressorModel] with Logging {

  private lazy implicit val psClient: AngelPSClient = synchronized {
    DriverContext.get().getAngelClient
  }

  override def read: MLReader[AngelRegressorModel] = new AngelSaverLoader.AngelModelReader[AngelRegressorModel]()
}
