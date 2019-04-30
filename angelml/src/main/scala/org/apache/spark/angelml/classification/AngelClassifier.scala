package org.apache.spark.angelml.classification

import java.util

import com.tencent.angel.client.AngelPSClient
import com.tencent.angel.matrix.MatrixContext
import com.tencent.angel.ml.core.PSOptimizerProvider
import com.tencent.angel.ml.core.conf.MLCoreConf
import com.tencent.angel.ml.core.variable.VarState
import com.tencent.angel.ml.math2.utils.LabeledData
import com.tencent.angel.sona.core._
import com.tencent.angel.sona.util.ConfUtils
import org.apache.spark.angelml.PredictorParams
import org.apache.spark.angelml.common._
import org.apache.spark.angelml.evaluation.evaluating.{BinaryClassificationSummaryImpl, MultiClassificationSummaryImpl}
import org.apache.spark.angelml.evaluation.training.ClassificationTrainingStat
import org.apache.spark.angelml.evaluation.{ClassificationSummary, TrainingStat}
import org.apache.spark.angelml.linalg.Vector
import org.apache.spark.angelml.metaextract.{AngelFeatureMeta, AngelIntLabelMeta}
import org.apache.spark.angelml.param.shared.HasProbabilityCol
import org.apache.spark.angelml.param.{AngelGraphParams, AngelOptParams, HasNumClasses, ParamMap}
import org.apache.spark.angelml.util.DataUtils.Example
import org.apache.spark.angelml.util._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.{DoubleType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.storage.StorageLevel

class AngelClassifier(override val uid: String)
  extends Classifier[Vector, AngelClassifier, AngelClassifierModel]
    with AngelGraphParams with AngelOptParams with HasNumClasses with ClassifierParams
    with DefaultParamsWritable with Logging {
  private var sparkSession: SparkSession = _
  private implicit val psClient: AngelPSClient = DriverContext.get().getAngelClient
  private val sparkEnvCtx: SparkEnvContext = DriverContext.get().sparkEnvContext
  implicit var bcValue: Broadcast[ExecutorContext] = _

  def this() = {
    this(Identifiable.randomUID("AngelClassification_"))
  }

  def setNumClasses(value: Int): this.type = setInternal(numClasses, value)

  setDefault(numClasses -> MLCoreConf.DEFAULT_ML_NUM_CLASS)

  override def updateFromProgramSetting(): this.type = {
    sharedConf.set(MLCoreConf.ML_IS_DATA_SPARSE, getIsSparse.toString)
    sharedConf.set(MLCoreConf.ML_MODEL_TYPE, getModelType)
    sharedConf.set(MLCoreConf.ML_FIELD_NUM, getNumField.toString)

    sharedConf.set(MLCoreConf.ML_EPOCH_NUM, getMaxIter.toString)
    sharedConf.set(MLCoreConf.ML_FEATURE_INDEX_RANGE, getNumFeature.toString)
    sharedConf.set(MLCoreConf.ML_NUM_CLASS, getNumClasses.toString)
    sharedConf.set(MLCoreConf.ML_LEARN_RATE, getLearningRate.toString)
    sharedConf.set(MLCoreConf.ML_OPTIMIZER_JSON_PROVIDER, classOf[PSOptimizerProvider].getName)
    sharedConf.set(MLCoreConf.ML_NUM_UPDATE_PER_EPOCH, getNumBatch.toString)
    sharedConf.set(MLCoreConf.ML_OPT_DECAY_CLASS_NAME, getDecayClass.toString)
    sharedConf.set(MLCoreConf.ML_OPT_DECAY_ALPHA, getDecayAlpha.toString)
    sharedConf.set(MLCoreConf.ML_OPT_DECAY_BETA, getDecayBeta.toString)
    sharedConf.set(MLCoreConf.ML_OPT_DECAY_INTERVALS, getDecayIntervals.toString)
    sharedConf.set(MLCoreConf.ML_OPT_DECAY_ON_BATCH, getDecayOnBatch.toString)

    this
  }

  override protected def train(dataset: Dataset[_]): AngelClassifierModel = {
    sparkSession = dataset.sparkSession
    sharedConf.set(ConfUtils.ALGO_TYPE, "class")

    // 1. trans Dataset to RDD
    val w = if (!isDefined(weightCol) || $(weightCol).isEmpty) lit(1.0) else col($(weightCol))
    val instances: RDD[Example] =
      dataset.select(col($(labelCol)), w, col($(featuresCol))).rdd.map {
        case Row(label: Double, weight: Double, features: Vector) => Example(label, weight, features)
      }

    val numTask = instances.getNumPartitions
    psClient.setTaskNum(numTask)

    // persist RDD if StorageLevel is NONE
    val handlePersistence = dataset.storageLevel == StorageLevel.NONE
    if (handlePersistence) instances.persist(StorageLevel.MEMORY_AND_DISK)

    // 2. create Instrumentation for log info
    val instr = Instrumentation.create(this, instances)
    instr.logParams(this, maxIter)

    // 3. calculate statistics for data set
    val (featSummarizer, labelSummarizer) = {
      val seqOp = (c: (AngelFeatureMeta, AngelIntLabelMeta), instance: Example) =>
        (c._1.add(instance), c._2.add(instance))

      val combOp = (c1: (AngelFeatureMeta, AngelIntLabelMeta),
                    c2: (AngelFeatureMeta, AngelIntLabelMeta)) =>
        (c1._1.merge(c2._1), c1._2.merge(c2._2))

      instances.treeAggregate(
        (new AngelFeatureMeta, new AngelIntLabelMeta)
      )(seqOp, combOp, $(aggregationDepth))
    }


    val maxIndex = featSummarizer.maxIndex
    instr.logNamedValue("maxIndex", maxIndex)
    if (getNumFeature < maxIndex + 1) {
      if (getNumFeature != -1) {
        log.warn(s"MaxFeatureIndex specified is smaller the the actual ${maxIndex + 1} !")
      } else {
        log.info(s"MaxFeatureIndex is set to ${maxIndex + 1} !")
      }

      setNumFeature(maxIndex + 1)
    } else {
      log.info(s"MaxFeatureIndex is ${maxIndex + 1} !")
    }
    implicit val dim: Long = getNumFeature

    val minIndex = featSummarizer.minIndex
    instr.logNamedValue("minIndex", minIndex)
    require(minIndex >= 0, "the min index must be >= 0")

    // update numFeatures, that is mode size
    sharedConf.set(MLCoreConf.ML_MODEL_SIZE, featSummarizer.validateIndexCount.toString)

    setNumClasses(labelSummarizer.numClasses)
    require(getNumClasses > 0, "the min numClasses must be > 0")

    if (getIsSparse != featSummarizer.isSparse) {
      setIsSparse(featSummarizer.isSparse)
    }

    // update sharedConf
    finalizeConf(psClient)

    bcValue = instances.context.broadcast(
      ExecutorContext(sharedConf, numTask, featSummarizer.partitionStat))
    DriverContext.get().registerBroadcastVariables(bcValue)

    /** *******************************************************************************************/

    val manifoldBuilder = new ManifoldBuilder(instances, getNumBatch)
    val manifoldRDD = manifoldBuilder.manifoldRDD()

    if (handlePersistence) instances.unpersist()

    val globalRunStat: ClassificationTrainingStat = new ClassificationTrainingStat(getNumClasses)
    val sparkModel: AngelClassifierModel = copyValues(
      new AngelClassifierModel(this.uid, getModelName),
      this.extractParamMap())

    sparkModel.setBCValue(bcValue)

    val angelModel = sparkModel.angelModel

    angelModel.buildNetwork()

    val startCreate = System.currentTimeMillis()
    angelModel.createMatrices(sparkEnvCtx)
    val finishedCreate = System.currentTimeMillis()
    globalRunStat.setCreateTime(finishedCreate - startCreate)

    DriverContext.get().createAndInitPSAgent

    if (getIncTrain) {
      val path = getInitModelPath
      require(path.nonEmpty, "InitModelPath is null or empty")

      val startLoad = System.currentTimeMillis()
      angelModel.loadModel(sparkEnvCtx, MLUtils.getHDFSPath(path), null)
      val finishedLoad = System.currentTimeMillis()
      globalRunStat.setLoadTime(finishedLoad - startLoad)
    } else {
      val startInit = System.currentTimeMillis()
      angelModel.init(SparkEnvContext(null))
      val finishedInit = System.currentTimeMillis()
      globalRunStat.setInitTime(finishedInit - startInit)
    }

    angelModel.setState(VarState.Ready)

    /** training **********************************************************************************/
    (0 until getMaxIter).foreach { epoch =>
      globalRunStat.clearStat().setAvgLoss(0.0).setNumSamples(0)
      manifoldRDD.foreach { batch: RDD[Array[LabeledData]] =>
        // training one batch
        val trainer = new Trainer(bcValue, epoch)
        val runStat = batch.map(miniBatch => trainer.trainOneBatch(miniBatch))
          .reduce(TrainingStat.mergeInBatch)

        // those code executor on driver
        val startUpdate = System.currentTimeMillis()
        angelModel.update(epoch, 1)
        val finishedUpdate = System.currentTimeMillis()
        runStat.setUpdateTime(finishedUpdate - startUpdate)

        globalRunStat.mergeMax(runStat)
      }

      globalRunStat.addHistLoss()
      println(globalRunStat.printString())
    }

    /** *******************************************************************************************/

    instr.logInfo(globalRunStat.printString())
    manifoldBuilder.foldedRDD.unpersist()

    sparkModel.setSummary(Some(globalRunStat))
    instr.logSuccess()

    sparkModel
  }

  override def copy(extra: ParamMap): AngelClassifier = defaultCopy(extra)
}


object AngelClassifier extends DefaultParamsReadable[AngelClassifier] with Logging {

  override def load(path: String): AngelClassifier = super.load(path)

}


class AngelClassifierModel(override val uid: String, override val angelModelName: String)
  extends ClassificationModel[Vector, AngelClassifierModel] with AngelSparkModel
    with HasProbabilityCol with PredictorParams with MLWritable with Logging {
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

  override def predict(features: Vector): Double = ???

  override protected def predictRaw(features: Vector): Vector = ???
}

object AngelClassifierModel extends MLReadable[AngelClassifierModel] with Logging {
  private lazy implicit val psClient: AngelPSClient = synchronized {
    DriverContext.get().getAngelClient
  }

  override def read: MLReader[AngelClassifierModel] = new AngelSaverLoader
  .AngelModelReader[AngelClassifierModel]()
}
