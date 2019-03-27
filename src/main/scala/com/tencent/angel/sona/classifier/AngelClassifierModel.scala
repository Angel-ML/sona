package com.tencent.angel.sona.classifier

import com.tencent.angel.client.AngelPSClient
import com.tencent.angel.ml.core.PSOptimizerProvider
import com.tencent.angel.ml.core.conf.MLCoreConf
import org.apache.spark.internal.Logging
import org.apache.spark.ml.APredictorParams
import org.apache.spark.ml.classification.ClassificationModel
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.HasProbabilityCol
import org.apache.spark.ml.util._
import org.apache.spark.rdd.RDD
import com.tencent.angel.sona.common.measure.ClassificationSummary
import com.tencent.angel.sona.common.measure.evaluating.{BinaryClassificationSummaryImpl, MultiClassificationSummaryImpl}
import com.tencent.angel.sona.common.{AngelSaverLoader, AngelSparkModel, Predictor}
import com.tencent.angel.sona.core._
import org.apache.spark.sql.types.{DoubleType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}


class AngelClassifierModel(override val uid: String, override val angelModelName: String)
  extends ClassificationModel[Vector, AngelClassifierModel] with AngelSparkModel
    with HasProbabilityCol with APredictorParams with MLWritable with Logging {
  @transient implicit override val psClient: AngelPSClient = DriverContext.get().getAngelClient
  override val numFeatures: Int = sharedConf.getInt(MLCoreConf.ML_FEATURE_INDEX_RANGE, -1)
  override val numClasses: Int = sharedConf.getInt(MLCoreConf.ML_NUM_CLASS, -1)

  def setProbabilityCol(value: String): this.type = setInternal(probabilityCol, value)

  override def updateFromProgramSetting(): this.type = {
    sharedConf.set(MLCoreConf.ML_IS_DATA_SPARSE, getIsSparse.toString)
    sharedConf.set(MLCoreConf.ML_MODEL_TYPE, getModelType)
    sharedConf.set(MLCoreConf.ML_FIELD_NUM, getNumField.toString)

    sharedConf.set(MLCoreConf.ML_FEATURE_INDEX_RANGE, getNumFeature.toString)
    sharedConf.set(MLCoreConf.ML_OPTIMIZER_JSON_PROVIDER, classOf[PSOptimizerProvider].getName)

    this
  }

  def findSummaryModel(): (AngelClassifierModel, String, String) = {
    val model = if ($(probabilityCol).isEmpty && $(predictionCol).isEmpty) {
      copy(ParamMap.empty)
        .setProbabilityCol("probability_" + java.util.UUID.randomUUID.toString)
        .setPredictionCol("prediction_" + java.util.UUID.randomUUID.toString)
    } else if ($(probabilityCol).isEmpty) {
      copy(ParamMap.empty).setProbabilityCol("probability_" + java.util.UUID.randomUUID.toString)
    } else if ($(predictionCol).isEmpty) {
      copy(ParamMap.empty).setPredictionCol("prediction_" + java.util.UUID.randomUUID.toString)
    } else {
      this
    }
    (model, model.getProbabilityCol, model.getPredictionCol)
  }

  def evaluate(dataset: Dataset[_]): ClassificationSummary = {
    val taskNum = dataset.rdd.getNumPartitions
    setNumTask(taskNum)

    // Handle possible missing or invalid prediction columns
    val (summaryModel, probabilityColName, predictionColName) = findSummaryModel()
    if (numClasses > 2) {
      new MultiClassificationSummaryImpl(summaryModel.transform(dataset),
        predictionColName, $(labelCol))
    } else {
      new BinaryClassificationSummaryImpl(summaryModel.transform(dataset),
        probabilityColName, $(labelCol))
    }
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema, logging = true)

    val taskNum = dataset.rdd.getNumPartitions
    setNumTask(taskNum)

    val featIdx: Int = dataset.schema.fieldIndex($(featuresCol))

    val probabilityColName = if ($(probabilityCol).isEmpty) {
      val value = s"probability_${java.util.UUID.randomUUID.toString}"
      setDefault(probabilityCol, value)
      value
    } else {
      $(probabilityCol)
    }

    val predictionColName = if ($(predictionCol).isEmpty) {
      val value = s"prediction_${java.util.UUID.randomUUID.toString}"
      setDefault(predictionCol, value)
      value
    } else {
      $(predictionCol)
    }

    if (bcValue == null) {
      finalizeConf(psClient)
      bcValue = dataset.rdd.context.broadcast(ExecutorContext(sharedConf, taskNum, null))
      DriverContext.get().registerBroadcastVariables(bcValue)
    }

    val predictor = new Predictor(bcValue, featIdx, probabilityColName, predictionColName)

    val newSchema: StructType = dataset.schema
      .add(probabilityColName, DoubleType)
      .add(predictionColName, DoubleType)

    val rddRow = dataset.rdd.asInstanceOf[RDD[Row]]
    val rddWithPredicted = rddRow.mapPartitions(predictor.predictRDD, preservesPartitioning = true)
    dataset.sparkSession.createDataFrame(rddWithPredicted, newSchema)
  }

  override def write: MLWriter = new AngelSaverLoader.AngelModelWriter(this)

  override def copy(extra: ParamMap): AngelClassifierModel = defaultCopy(extra)

  override protected def predict(features: Vector): Double = {
    val raw = predictRaw(features)
    if (numClasses == 2) {
      if (raw.argmax == 0) -1.0 else 1.0
    } else {
      raw.argmax
    }
  }

  override protected def predictRaw(features: Vector): Vector = ???
}

object AngelClassifierModel extends MLReadable[AngelClassifierModel] with Logging {
  private lazy implicit val psClient: AngelPSClient = synchronized {
    DriverContext.get().getAngelClient
  }

  override def read: MLReader[AngelClassifierModel] = new AngelSaverLoader
  .AngelModelReader[AngelClassifierModel]()
}
