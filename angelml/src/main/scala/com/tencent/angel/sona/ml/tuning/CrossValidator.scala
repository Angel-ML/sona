/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tencent.angel.sona.ml.tuning

import java.util.{Locale, List => JList}
import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import org.apache.hadoop.fs.Path
import com.tencent.angel.sona.ml.util._
import org.json4s.DefaultFormats
import org.apache.spark.internal.Logging
import com.tencent.angel.sona.ml.{Estimator, Model}
import com.tencent.angel.sona.ml.evaluation.Evaluator
import com.tencent.angel.sona.ml.param.{IntParam, ParamMap, ParamValidators}
import com.tencent.angel.sona.ml.param.shared.{HasCollectSubModels, HasParallelism}
import com.tencent.angel.sona.ml.util.Instrumentation.instrumented
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util._

/**
  * Params for [[CrossValidator]] and [[CrossValidatorModel]].
  */
private[sona] trait CrossValidatorParams extends ValidatorParams {
  /**
    * Param for number of folds for cross validation.  Must be &gt;= 2.
    * Default: 3
    *
    * @group param
    */
  val numFolds: IntParam = new IntParam(this, "numFolds",
    "number of folds for cross validation (>= 2)", ParamValidators.gtEq(2))

  /** @group getParam */
  def getNumFolds: Int = $(numFolds)

  setDefault(numFolds -> 3)
}

/**
  * K-fold cross validation performs model selection by splitting the dataset into a set of
  * non-overlapping randomly partitioned folds which are used as separate training and test datasets
  * e.g., with k=3 folds, K-fold cross validation will generate 3 (training, test) dataset pairs,
  * each of which uses 2/3 of the data for training and 1/3 for testing. Each fold is used as the
  * test set exactly once.
  */

class CrossValidator(override val uid: String)
  extends Estimator[CrossValidatorModel]
    with CrossValidatorParams with HasParallelism with HasCollectSubModels
    with MLWritable with Logging {


  def this() = this(Identifiable.randomUID("cv"))

  /** @group setParam */

  def setEstimator(value: Estimator[_]): this.type = set(estimator, value)

  /** @group setParam */

  def setEstimatorParamMaps(value: Array[ParamMap]): this.type = set(estimatorParamMaps, value)

  /** @group setParam */

  def setEvaluator(value: Evaluator): this.type = set(evaluator, value)

  /** @group setParam */

  def setNumFolds(value: Int): this.type = set(numFolds, value)

  /** @group setParam */

  def setSeed(value: Long): this.type = set(seed, value)

  /**
    * Set the maximum level of parallelism to evaluate models in parallel.
    * Default is 1 for serial evaluation
    *
    * @group expertSetParam
    */

  def setParallelism(value: Int): this.type = set(parallelism, value)

  /**
    * Whether to collect submodels when fitting. If set, we can get submodels from
    * the returned model.
    *
    * Note: If set this param, when you save the returned model, you can set an option
    * "persistSubModels" to be "true" before saving, in order to save these submodels.
    * You can check documents of
    * {@link org.apache.spark.ml.tuning.CrossValidatorModel.CrossValidatorModelWriter}
    * for more information.
    *
    * @group expertSetParam
    */

  def setCollectSubModels(value: Boolean): this.type = set(collectSubModels, value)


  override def fit(dataset: Dataset[_]): CrossValidatorModel = instrumented { instr =>
    val schema = dataset.schema
    transformSchema(schema, logging = true)
    val sparkSession = dataset.sparkSession
    val est = $(estimator)
    val eval = $(evaluator)
    val epm = $(estimatorParamMaps)

    // Create execution context based on $(parallelism)
    val executionContext = getExecutionContext

    instr.logPipelineStage(this)
    instr.logDataset(dataset)
    instr.logParams(this, numFolds, seed, parallelism)
    logTuningParams(instr)

    val collectSubModelsParam = $(collectSubModels)

    var subModels: Option[Array[Array[Model[_]]]] = if (collectSubModelsParam) {
      Some(Array.fill($(numFolds))(Array.fill[Model[_]](epm.length)(null)))
    } else None

    // Compute metrics for each model over each split
    val splits = MLUtils.kFold(dataset.toDF.rdd, $(numFolds), $(seed))
    val metrics = splits.zipWithIndex.map { case ((training, validation), splitIndex) =>
      val trainingDataset = sparkSession.createDataFrame(training, schema).cache()
      val validationDataset = sparkSession.createDataFrame(validation, schema).cache()
      instr.logDebug(s"Train split $splitIndex with multiple sets of parameters.")

      // Fit models in a Future for training in parallel
      val foldMetricFutures = epm.zipWithIndex.map { case (paramMap, paramIndex) =>
        Future[Double] {
          val model = est.fit(trainingDataset, paramMap).asInstanceOf[Model[_]]
          if (collectSubModelsParam) {
            subModels.get(splitIndex)(paramIndex) = model
          }
          // TODO: duplicate evaluator to take extra params from input
          val metric = eval.evaluate(model.transform(validationDataset, paramMap))
          instr.logDebug(s"Got metric $metric for model trained with $paramMap.")
          metric
        }(executionContext)
      }

      // Wait for metrics to be calculated
      val foldMetrics = foldMetricFutures.map(ThreadUtil.awaitResult(_, Duration.Inf))

      // Unpersist training & validation set once all metrics have been produced
      trainingDataset.unpersist()
      validationDataset.unpersist()
      foldMetrics
    }.transpose.map(_.sum / $(numFolds)) // Calculate average metric over all splits

    instr.logInfo(s"Average cross-validation metrics: ${metrics.toSeq}")
    val (bestMetric, bestIndex) =
      if (eval.isLargerBetter) metrics.zipWithIndex.maxBy(_._1)
      else metrics.zipWithIndex.minBy(_._1)
    instr.logInfo(s"Best set of parameters:\n${epm(bestIndex)}")
    instr.logInfo(s"Best cross-validation metric: $bestMetric.")
    val bestModel = est.fit(dataset, epm(bestIndex)).asInstanceOf[Model[_]]
    copyValues(new CrossValidatorModel(uid, bestModel, metrics)
      .setSubModels(subModels).setParent(this))
  }


  override def transformSchema(schema: StructType): StructType = transformSchemaImpl(schema)


  override def copy(extra: ParamMap): CrossValidator = {
    val copied = defaultCopy(extra).asInstanceOf[CrossValidator]
    if (copied.isDefined(estimator)) {
      copied.setEstimator(copied.getEstimator.copy(extra))
    }
    if (copied.isDefined(evaluator)) {
      copied.setEvaluator(copied.getEvaluator.copy(extra))
    }
    copied
  }

  // Currently, this only works if all [[Param]]s in [[estimatorParamMaps]] are simple types.
  // E.g., this may fail if a [[Param]] is an instance of an [[Estimator]].
  // However, this case should be unusual.

  override def write: MLWriter = new CrossValidator.CrossValidatorWriter(this)
}


object CrossValidator extends MLReadable[CrossValidator] {


  override def read: MLReader[CrossValidator] = new CrossValidatorReader


  override def load(path: String): CrossValidator = super.load(path)

  private[CrossValidator] class CrossValidatorWriter(instance: CrossValidator) extends MLWriter {

    ValidatorParams.validateParams(instance)

    override protected def saveImpl(path: String): Unit =
      ValidatorParams.saveImpl(path, instance, sc)
  }

  private class CrossValidatorReader extends MLReader[CrossValidator] {

    /** Checked against metadata when loading model */
    private val className = classOf[CrossValidator].getName

    override def load(path: String): CrossValidator = {
      implicit val format: DefaultFormats = DefaultFormats

      val (metadata, estimator, evaluator, estimatorParamMaps) =
        ValidatorParams.loadImpl(path, sc, className)
      val cv = new CrossValidator(metadata.uid)
        .setEstimator(estimator)
        .setEvaluator(evaluator)
        .setEstimatorParamMaps(estimatorParamMaps)
      metadata.getAndSetParams(cv, skipParams = Option(List("estimatorParamMaps")))
      cv
    }
  }

}

/**
  * CrossValidatorModel contains the model with the highest average cross-validation
  * metric across folds and uses this model to transform input data. CrossValidatorModel
  * also tracks the metrics for each param map evaluated.
  *
  * @param bestModel  The best model selected from k-fold cross validation.
  * @param avgMetrics Average cross-validation metrics for each paramMap in
  *                   `CrossValidator.estimatorParamMaps`, in the corresponding order.
  */

class CrossValidatorModel private[sona](
                                          override val uid: String,
                                          val bestModel: Model[_],
                                          val avgMetrics: Array[Double])
  extends Model[CrossValidatorModel] with CrossValidatorParams with MLWritable {

  /** A Python-friendly auxiliary constructor. */
  private[sona] def this(uid: String, bestModel: Model[_], avgMetrics: JList[Double]) = {
    this(uid, bestModel, avgMetrics.asScala.toArray)
  }

  private var _subModels: Option[Array[Array[Model[_]]]] = None

  private[tuning] def setSubModels(subModels: Option[Array[Array[Model[_]]]])
  : CrossValidatorModel = {
    _subModels = subModels
    this
  }

  // A Python-friendly auxiliary method
  private[tuning] def setSubModels(subModels: JList[JList[Model[_]]])
  : CrossValidatorModel = {
    _subModels = if (subModels != null) {
      Some(subModels.asScala.toArray.map(_.asScala.toArray))
    } else {
      None
    }
    this
  }

  /**
    * @return submodels represented in two dimension array. The index of outer array is the
    *         fold index, and the index of inner array corresponds to the ordering of
    *         estimatorParamMaps
    * @throws IllegalArgumentException if subModels are not available. To retrieve subModels,
    *                                  make sure to set collectSubModels to true before fitting.
    */

  def subModels: Array[Array[Model[_]]] = {
    require(_subModels.isDefined, "subModels not available, To retrieve subModels, make sure " +
      "to set collectSubModels to true before fitting.")
    _subModels.get
  }


  def hasSubModels: Boolean = _subModels.isDefined


  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema, logging = true)
    bestModel.transform(dataset)
  }


  override def transformSchema(schema: StructType): StructType = {
    bestModel.transformSchema(schema)
  }


  override def copy(extra: ParamMap): CrossValidatorModel = {
    val copied = new CrossValidatorModel(
      uid,
      bestModel.copy(extra).asInstanceOf[Model[_]],
      avgMetrics.clone()
    ).setSubModels(CrossValidatorModel.copySubModels(_subModels))
    copyValues(copied, extra).setParent(parent)
  }


  override def write: CrossValidatorModel.CrossValidatorModelWriter = {
    new CrossValidatorModel.CrossValidatorModelWriter(this)
  }
}


object CrossValidatorModel extends MLReadable[CrossValidatorModel] {

  private[CrossValidatorModel] def copySubModels(subModels: Option[Array[Array[Model[_]]]])
  : Option[Array[Array[Model[_]]]] = {
    subModels.map(_.map(_.map(_.copy(ParamMap.empty).asInstanceOf[Model[_]])))
  }


  override def read: MLReader[CrossValidatorModel] = new CrossValidatorModelReader


  override def load(path: String): CrossValidatorModel = super.load(path)

  /**
    * Writer for CrossValidatorModel.
    *
    * @param instance CrossValidatorModel instance used to construct the writer
    *
    *                 CrossValidatorModelWriter supports an option "persistSubModels", with possible values
    *                 "true" or "false". If you set the collectSubModels Param before fitting, then you can
    *                 set "persistSubModels" to "true" in order to persist the subModels. By default,
    *                 "persistSubModels" will be "true" when subModels are available and "false" otherwise.
    *                 If subModels are not available, then setting "persistSubModels" to "true" will cause
    *                 an exception.
    */

  final class CrossValidatorModelWriter private[tuning](
                                                         instance: CrossValidatorModel) extends MLWriter {

    ValidatorParams.validateParams(instance)

    override protected def saveImpl(path: String): Unit = {
      val persistSubModelsParam = optionMap.getOrElse("persistsubmodels",
        if (instance.hasSubModels) "true" else "false")

      require(Array("true", "false").contains(persistSubModelsParam.toLowerCase(Locale.ROOT)),
        s"persistSubModels option value ${persistSubModelsParam} is invalid, the possible " +
          "values are \"true\" or \"false\"")
      val persistSubModels = persistSubModelsParam.toBoolean

      import org.json4s.JsonDSL._
      val extraMetadata = ("avgMetrics" -> instance.avgMetrics.toSeq) ~
        ("persistSubModels" -> persistSubModels)
      ValidatorParams.saveImpl(path, instance, sc, Some(extraMetadata))
      val bestModelPath = new Path(path, "bestModel").toString
      instance.bestModel.asInstanceOf[MLWritable].save(bestModelPath)
      if (persistSubModels) {
        require(instance.hasSubModels, "When persisting tuning models, you can only set " +
          "persistSubModels to true if the tuning was done with collectSubModels set to true. " +
          "To save the sub-models, try rerunning fitting with collectSubModels set to true.")
        val subModelsPath = new Path(path, "subModels")
        for (splitIndex <- 0 until instance.getNumFolds) {
          val splitPath = new Path(subModelsPath, s"fold${splitIndex.toString}")
          for (paramIndex <- 0 until instance.getEstimatorParamMaps.length) {
            val modelPath = new Path(splitPath, paramIndex.toString).toString
            instance.subModels(splitIndex)(paramIndex).asInstanceOf[MLWritable].save(modelPath)
          }
        }
      }
    }
  }

  private class CrossValidatorModelReader extends MLReader[CrossValidatorModel] {

    /** Checked against metadata when loading model */
    private val className = classOf[CrossValidatorModel].getName

    override def load(path: String): CrossValidatorModel = {
      implicit val format = DefaultFormats

      val (metadata, estimator, evaluator, estimatorParamMaps) =
        ValidatorParams.loadImpl(path, sc, className)
      val numFolds = (metadata.params \ "numFolds").extract[Int]
      val bestModelPath = new Path(path, "bestModel").toString
      val bestModel = DefaultParamsReader.loadParamsInstance[Model[_]](bestModelPath, sc)
      val avgMetrics = (metadata.metadata \ "avgMetrics").extract[Seq[Double]].toArray
      val persistSubModels = (metadata.metadata \ "persistSubModels")
        .extractOrElse[Boolean](false)

      val subModels: Option[Array[Array[Model[_]]]] = if (persistSubModels) {
        val subModelsPath = new Path(path, "subModels")
        val _subModels = Array.fill(numFolds)(Array.fill[Model[_]](
          estimatorParamMaps.length)(null))
        for (splitIndex <- 0 until numFolds) {
          val splitPath = new Path(subModelsPath, s"fold${splitIndex.toString}")
          for (paramIndex <- 0 until estimatorParamMaps.length) {
            val modelPath = new Path(splitPath, paramIndex.toString).toString
            _subModels(splitIndex)(paramIndex) =
              DefaultParamsReader.loadParamsInstance(modelPath, sc)
          }
        }
        Some(_subModels)
      } else None

      val model = new CrossValidatorModel(metadata.uid, bestModel, avgMetrics)
        .setSubModels(subModels)
      model.set(model.estimator, estimator)
        .set(model.evaluator, evaluator)
        .set(model.estimatorParamMaps, estimatorParamMaps)
      metadata.getAndSetParams(model, skipParams = Option(List("estimatorParamMaps")))
      model
    }
  }

}
