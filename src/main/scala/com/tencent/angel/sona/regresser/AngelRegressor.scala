package com.tencent.angel.sona.regresser

import com.tencent.angel.client.AngelPSClient
import com.tencent.angel.ml.core.PSOptimizerProvider
import com.tencent.angel.ml.core.conf.MLCoreConf
import com.tencent.angel.ml.core.variable.VarState
import com.tencent.angel.ml.math2.utils.LabeledData
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util._
import org.apache.spark.ml.{AInstrumentation, APredictorParams, ARegressor, SPKMLUtils}
import org.apache.spark.rdd.RDD
import com.tencent.angel.sona.common.measure.TrainingStat
import com.tencent.angel.sona.common.measure.training.RegressionTrainingStat
import com.tencent.angel.sona.common.metaextract.{AngelFeatureMeta, AngelFloatLabelMeta}
import com.tencent.angel.sona.common.params.{AngelGraphParams, AngelOptParams}
import com.tencent.angel.sona.common.{ManifoldBuilder, Trainer}
import com.tencent.angel.sona.core._
import com.tencent.angel.sona.utils.ConfUtils
import com.tencent.angel.sona.utils.DataUtils.Example
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.storage.StorageLevel


class AngelRegressor(override val uid: String)
  extends ARegressor[Vector, AngelRegressor, AngelRegressorModel]
    with AngelGraphParams with AngelOptParams with APredictorParams
    with DefaultParamsWritable with Logging {
  private var sparkSession: SparkSession = _
  private implicit val psClient: AngelPSClient = DriverContext.get().getAngelClient
  private val sparkEnvCtx: SparkEnvContext = DriverContext.get().sparkEnvContext
  implicit var bcValue: Broadcast[ExecutorContext] = _

  def this() = {
    this(Identifiable.randomUID("AngelRegression_"))
  }

  override def updateFromProgramSetting(): this.type = {
    sharedConf.set(MLCoreConf.ML_IS_DATA_SPARSE, getIsSparse.toString)
    sharedConf.set(MLCoreConf.ML_MODEL_TYPE, getModelType)
    sharedConf.set(MLCoreConf.ML_FIELD_NUM, getNumField.toString)

    sharedConf.set(MLCoreConf.ML_EPOCH_NUM, getMaxIter.toString)
    sharedConf.set(MLCoreConf.ML_FEATURE_INDEX_RANGE, getNumFeature.toString)
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

  override protected def train(dataset: Dataset[_]): AngelRegressorModel = {
    sparkSession = dataset.sparkSession
    sharedConf.set(ConfUtils.ALGO_TYPE, "regression")

    // 1. trans Dataset to RDD
    val w = if (!isDefined(weightCol) || $(weightCol).isEmpty) lit(1.0) else col($(weightCol))
    val instances: RDD[Example] =
      dataset.select(col($(labelCol)), w, col($(featuresCol))).rdd.map {
        case Row(label: Double, weight: Double, features: Vector) =>
          Example(label, weight, features)
      }

    val numTask = instances.getNumPartitions
    psClient.setTaskNum(numTask)

    // persist RDD if StorageLevel is NONE
    val handlePersistence = dataset.storageLevel == StorageLevel.NONE
    if (handlePersistence) instances.persist(StorageLevel.MEMORY_AND_DISK)

    // 2. create Instrumentation for log info
    val instr = AInstrumentation.create(this, instances)
    instr.logParams(maxIter)

    // 3. calculate statistics for data set
    val (featSummarizer, labelSummarizer) = {
      val seqOp = (c: (AngelFeatureMeta, AngelFloatLabelMeta), instance: Example) =>
        (c._1.add(instance), c._2.add(instance))

      val combOp = (c1: (AngelFeatureMeta, AngelFloatLabelMeta),
                    c2: (AngelFeatureMeta, AngelFloatLabelMeta)) =>
        (c1._1.merge(c2._1), c1._2.merge(c2._2))

      instances.treeAggregate(
        (new AngelFeatureMeta, new AngelFloatLabelMeta)
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
    // update numFeatures, that is mode size
    sharedConf.set(MLCoreConf.ML_MODEL_SIZE, featSummarizer.validateIndexCount.toString)

    // instr.logNamedValue("validateIndexCount", numFeatures)

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

    val globalRunStat: RegressionTrainingStat = new RegressionTrainingStat()
    val sparkModel: AngelRegressorModel = copyValues(
      new AngelRegressorModel(this.uid, getModelName),
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
      angelModel.loadModel(sparkEnvCtx, SPKMLUtils.getHDFSPath(path))
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

    instr.log(globalRunStat.printString())
    manifoldBuilder.foldedRDD.unpersist()

    sparkModel.setSummary(Some(globalRunStat))
    instr.logSuccess(sparkModel)

    sparkModel
  }

  override def copy(extra: ParamMap): AngelRegressor = defaultCopy(extra)
}

object AngelRegressor extends DefaultParamsReadable[AngelRegressor] with Logging {

  override def load(path: String): AngelRegressor = super.load(path)

}
