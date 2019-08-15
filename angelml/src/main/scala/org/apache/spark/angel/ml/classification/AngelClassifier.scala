package org.apache.spark.angel.ml.classification

import com.tencent.angel.client.AngelPSClient
import com.tencent.angel.ml.core.PSOptimizerProvider
import com.tencent.angel.ml.math2.utils.{LabeledData, RowType}
import com.tencent.angel.mlcore.conf.{MLCoreConf, SharedConf}
import com.tencent.angel.mlcore.variable.VarState
import com.tencent.angel.psagent.{PSAgent, PSAgentContext}
import com.tencent.angel.sona.core.{DriverContext, _}
import com.tencent.angel.sona.ml.AngeGraphModel
import com.tencent.angel.sona.util.ConfUtils
import org.apache.spark.angel.ml.common.{AngelSaverLoader, AngelSparkModel, ManifoldBuilder, Predictor, Trainer}
import org.apache.spark.angel.ml.evaluation.{ClassificationSummary, TrainingStat}
import org.apache.spark.angel.ml.evaluation.evaluating.{BinaryClassificationSummaryImpl, MultiClassificationSummaryImpl}
import org.apache.spark.angel.ml.evaluation.training.ClassificationTrainingStat
import org.apache.spark.angel.ml.{PredictorParams, linalg}
import org.apache.spark.angel.ml.linalg.{DenseVector, IntSparseVector, LongSparseVector, SparseVector}
import org.apache.spark.angel.ml.param.{AngelGraphParams, AngelOptParams, HasNumClasses, ParamMap}
import org.apache.spark.angel.ml.param.shared.HasProbabilityCol
import org.apache.spark.angel.ml.util.DataUtils.Example
import org.apache.spark.angel.ml.util.{DefaultParamsReadable, DefaultParamsWritable, FeatureStats, Identifiable, Instrumentation, MLReadable, MLReader, MLUtils, MLWritable, MLWriter}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.{DoubleType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.JavaConverters._

class AngelClassifier(override val uid: String)
  extends Classifier[linalg.Vector, AngelClassifier, AngelClassifierModel]
    with AngelGraphParams with AngelOptParams with HasNumClasses with ClassifierParams
    with DefaultParamsWritable with Logging {
  private var sparkSession: SparkSession = _
  private val driverCtx = DriverContext.get()
  private implicit val psClient: AngelPSClient = driverCtx.getAngelClient
  private implicit val psAgent: PSAgent = driverCtx.getPSAgent
  private val sparkMasterCtx: SparkMasterContext = driverCtx.sparkMasterContext
  override val sharedConf: SharedConf = driverCtx.sharedConf
  implicit var bcExeCtx: Broadcast[ExecutorContext] = _
  implicit var bcConf: Broadcast[SharedConf] = _
  private var angelModel: AngeGraphModel = _

  def this() = {
    this(Identifiable.randomUID("AngelClassification_"))
  }

  def setNumClass(value: Int): this.type = setInternal(numClass, value)

  setDefault(numClass -> MLCoreConf.DEFAULT_ML_NUM_CLASS)

  override def updateFromProgramSetting(): this.type = {
    sharedConf.set(MLCoreConf.ML_IS_DATA_SPARSE, getIsSparse.toString)
    sharedConf.set(MLCoreConf.ML_MODEL_TYPE, getModelType)
    sharedConf.set(MLCoreConf.ML_FEATURE_INDEX_RANGE, getNumFeature.toString)
    sharedConf.set(MLCoreConf.ML_NUM_CLASS, getNumClass.toString)
    sharedConf.set(MLCoreConf.ML_MODEL_SIZE, getModelSize.toString)
    sharedConf.set(MLCoreConf.ML_FIELD_NUM, getNumField.toString)

    sharedConf.set(MLCoreConf.ML_EPOCH_NUM, getMaxIter.toString)
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
        case Row(label: Double, weight: Double, features: linalg.Vector) => Example(label, weight, features)
      }

    val numTask = instances.getNumPartitions
    psClient.setTaskNum(numTask)

    bcExeCtx = instances.context.broadcast(ExecutorContext(sharedConf, numTask))
    DriverContext.get().registerBroadcastVariables(bcExeCtx)

    // persist RDD if StorageLevel is NONE
    val handlePersistence = dataset.storageLevel == StorageLevel.NONE
    if (handlePersistence) instances.persist(StorageLevel.MEMORY_AND_DISK)

    // 2. create Instrumentation for log info
    val instr = Instrumentation.create(this, instances)
    instr.logParams(this, maxIter)

    // 3. check data configs
    val example = instances.take(1).head.features

    // 3.1 NumFeature check
    if (example.size != getNumFeature && getNumFeature != -1) {
      // has set
      setNumFeatures(Math.max(example.size, getNumFeature))
      log.info("number of feature form data and algorithm setting does not match")
    } else if (example.size != getNumFeature && getNumFeature == -1) {
      // not set
      setDefault(numFeature, example.size)
      log.info("get number of feature form data")
    } else {
      log.info("number of feature form data and algorithm setting match")
    }
    instr.logNamedValue("NumFeatures", getNumFeature)

    // 3.2 better modelType default value for sona
    if (getModelSize == -1) {
      if (example.size < 1e6) {
        setDefault(modelType, RowType.T_DOUBLE_DENSE.toString)
      } else if (example.size < Int.MaxValue) {
        setDefault(modelType, RowType.T_DOUBLE_SPARSE.toString)
      } else {
        setDefault(modelType, RowType.T_DOUBLE_SPARSE_LONGKEY.toString)
      }
    } else {
      example match {
        case _: DenseVector =>
          setDefault(modelType, RowType.T_DOUBLE_DENSE.toString)
        case iv: IntSparseVector if iv.size <= (2.0 * getModelSize) =>
          setDefault(modelType, RowType.T_DOUBLE_DENSE.toString)
        case iv: IntSparseVector if iv.size > (2.0 * getModelSize) =>
          setDefault(modelType, RowType.T_DOUBLE_SPARSE.toString)
        case _: LongSparseVector =>
          setDefault(modelType, RowType.T_DOUBLE_SPARSE_LONGKEY.toString)
      }
    }

    // 3.3 ModelSize check && partitionStat
    val featureStats = new FeatureStats(uid, getModelType, bcExeCtx)
    val partitionStat = if (getModelSize == -1) {
      // not set
      example match {
        case v: DenseVector =>
          setModelSize(v.size)
          instances.mapPartitions(featureStats.partitionStats, preservesPartitioning = true)
            .reduce(featureStats.mergeMap).asScala.toMap
        case _: SparseVector =>
          featureStats.createPSMat(psClient, getNumFeature)
          val partitionStat_ = instances.mapPartitions(featureStats.partitionStatsWithPS, preservesPartitioning = true)
            .reduce(featureStats.mergeMap).asScala.toMap

          val numValidateFeatures = featureStats.getNumValidateFeatures(psAgent)
          setModelSize(numValidateFeatures)
          partitionStat_
      }
    } else {
      // has set
      instances.mapPartitions(featureStats.partitionStats, preservesPartitioning = true)
        .reduce(featureStats.mergeMap).asScala.toMap
    }

    // 3.4 input data format check and better modelType default value after model known
    example match {
      case _: DenseVector =>
        setIsSparse(false)
        setDefault(modelType, RowType.T_DOUBLE_DENSE.toString)
      case iv: IntSparseVector if iv.size <= (2.0 * getModelSize) =>
        setIsSparse(true)
        setDefault(modelType, RowType.T_DOUBLE_DENSE.toString)
      case iv: IntSparseVector if iv.size > (2.0 * getModelSize) =>
        setIsSparse(true)
        setDefault(modelType, RowType.T_DOUBLE_SPARSE.toString)
      case _: LongSparseVector =>
        setIsSparse(true)
        setDefault(modelType, RowType.T_DOUBLE_SPARSE_LONGKEY.toString)
    }

    // update sharedConf
    finalizeConf(psClient)
    bcConf = instances.context.broadcast(sharedConf)
    DriverContext.get().registerBroadcastVariables(bcConf)

    /** *******************************************************************************************/
    implicit val dim: Long = getNumFeature
    val manifoldBuilder = new ManifoldBuilder(instances, getNumBatch, partitionStat)
    val manifoldRDD = manifoldBuilder.manifoldRDD()

    val globalRunStat: ClassificationTrainingStat = new ClassificationTrainingStat(getNumClass)
    val sparkModel: AngelClassifierModel = copyValues(
      new AngelClassifierModel(this.uid, getModelName),
      this.extractParamMap())

    sparkModel.setBCValue(bcExeCtx)

    angelModel = sparkModel.angelModel

    angelModel.buildNetwork()

    val startCreate = System.currentTimeMillis()
    angelModel.createMatrices(sparkMasterCtx)
    PSAgentContext.get().getPsAgent.refreshMatrixInfo()
    val finishedCreate = System.currentTimeMillis()
    globalRunStat.setCreateTime(finishedCreate - startCreate)

    if (getIncTrain) {
      val path = getInitModelPath
      require(path.nonEmpty, "InitModelPath is null or empty")

      val startLoad = System.currentTimeMillis()
      angelModel.loadModel(sparkMasterCtx, MLUtils.getHDFSPath(path), null)
      val finishedLoad = System.currentTimeMillis()
      globalRunStat.setLoadTime(finishedLoad - startLoad)
    } else {
      val startInit = System.currentTimeMillis()
      angelModel.init(SparkMasterContext(null))
      val finishedInit = System.currentTimeMillis()
      globalRunStat.setInitTime(finishedInit - startInit)
    }

    angelModel.setState(VarState.Ready)

    /** training **********************************************************************************/
    (0 until getMaxIter).foreach { epoch =>
      globalRunStat.clearStat().setAvgLoss(0.0).setNumSamples(0)
      manifoldRDD.foreach { case batch: RDD[Array[LabeledData]] =>
        // training one batch
        val trainer = new Trainer(bcExeCtx, epoch, bcConf)
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

    sparkModel.setSummary(Some(globalRunStat))
    instr.logSuccess()

    sparkModel
  }

  override def copy(extra: ParamMap): AngelClassifier = defaultCopy(extra)

  def releaseAngelModel(): this.type = {
    if (angelModel != null) {
      angelModel.releaseMode(driverCtx.sparkWorkerContext)
    }

    angelModel = null
    this
  }
}


object AngelClassifier extends DefaultParamsReadable[AngelClassifier] with Logging {

  override def load(path: String): AngelClassifier = super.load(path)

}


class AngelClassifierModel(override val uid: String, override val angelModelName: String)
  extends ClassificationModel[linalg.Vector, AngelClassifierModel] with AngelSparkModel
    with HasProbabilityCol with PredictorParams with HasNumClasses with MLWritable with Logging {
  @transient implicit override val psClient: AngelPSClient = DriverContext.get().getAngelClient
  override lazy val numFeatures: Long = getNumFeature
  override lazy val numClasses: Int = getNumClass
  override val sharedConf: SharedConf = DriverContext.get().sharedConf


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
      bcValue = dataset.rdd.context.broadcast(ExecutorContext(sharedConf, taskNum))
      DriverContext.get().registerBroadcastVariables(bcValue)
    }

    if (bcConf == null) {
      finalizeConf(psClient)
      bcConf = dataset.rdd.context.broadcast(sharedConf)
      DriverContext.get().registerBroadcastVariables(bcConf)
    }

    val predictor = new Predictor(bcValue, featIdx, probabilityColName, predictionColName, bcConf)

    val newSchema: StructType = dataset.schema
      .add(probabilityColName, DoubleType)
      .add(predictionColName, DoubleType)

    val rddRow = dataset.rdd.asInstanceOf[RDD[Row]]
    val rddWithPredicted = rddRow.mapPartitions(predictor.predictRDD, preservesPartitioning = true)
    dataset.sparkSession.createDataFrame(rddWithPredicted, newSchema)
  }

  override def write: MLWriter = new AngelSaverLoader.AngelModelWriter(this)

  override def copy(extra: ParamMap): AngelClassifierModel = defaultCopy(extra)

  override def predict(features: linalg.Vector): Double = ???

  override protected def predictRaw(features: linalg.Vector): linalg.Vector = ???
}

object AngelClassifierModel extends MLReadable[AngelClassifierModel] with Logging {
  private lazy implicit val psClient: AngelPSClient = synchronized {
    DriverContext.get().getAngelClient
  }

  override def read: MLReader[AngelClassifierModel] = new AngelSaverLoader
  .AngelModelReader[AngelClassifierModel]()
}
