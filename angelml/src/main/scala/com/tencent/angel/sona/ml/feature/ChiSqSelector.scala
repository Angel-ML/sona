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

package com.tencent.angel.sona.ml.feature

import org.apache.hadoop.fs.Path
import com.tencent.angel.sona.ml
import com.tencent.angel.sona.ml.{Estimator, Model}
import com.tencent.angel.sona.ml.attribute.{Attribute, AttributeGroup, NominalAttribute}
import org.apache.spark.linalg
import org.apache.spark.linalg._
import com.tencent.angel.sona.ml.param.{DoubleParam, IntParam, Param, ParamMap, ParamValidators, Params}
import com.tencent.angel.sona.ml.param.shared.{HasFeaturesCol, HasLabelCol, HasOutputCol}
import com.tencent.angel.sona.ml.stat.Statistics
import com.tencent.angel.sona.ml.stat.test.ChiSqTestResult
import com.tencent.angel.sona.ml.util._
import org.apache.spark.sql.util.SchemaUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.util.DatasetUtil

import scala.collection.mutable

/**
  * Params for [[ChiSqSelector]] and [[ChiSqSelectorModel]].
  */
private[sona] trait ChiSqSelectorParams extends Params
  with HasFeaturesCol with HasOutputCol with HasLabelCol {

  /**
    * Number of features that selector will select, ordered by ascending p-value. If the
    * number of features is less than numTopFeatures, then this will select all features.
    * Only applicable when selectorType = "numTopFeatures".
    * The default value of numTopFeatures is 50.
    *
    * @group param
    */

  final val numTopFeatures = new IntParam(this, "numTopFeatures",
    "Number of features that selector will select, ordered by ascending p-value. If the" +
      " number of features is < numTopFeatures, then this will select all features.",
    ParamValidators.gtEq(1))
  setDefault(numTopFeatures -> 50)

  /** @group getParam */

  def getNumTopFeatures: Int = $(numTopFeatures)

  /**
    * Percentile of features that selector will select, ordered by statistics value descending.
    * Only applicable when selectorType = "percentile".
    * Default value is 0.1.
    *
    * @group param
    */

  final val percentile = new DoubleParam(this, "percentile",
    "Percentile of features that selector will select, ordered by ascending p-value.",
    ParamValidators.inRange(0, 1))
  setDefault(percentile -> 0.1)

  /** @group getParam */

  def getPercentile: Double = $(percentile)

  /**
    * The highest p-value for features to be kept.
    * Only applicable when selectorType = "fpr".
    * Default value is 0.05.
    *
    * @group param
    */

  final val fpr = new DoubleParam(this, "fpr", "The highest p-value for features to be kept.",
    ParamValidators.inRange(0, 1))
  setDefault(fpr -> 0.05)

  /** @group getParam */

  def getFpr: Double = $(fpr)

  /**
    * The upper bound of the expected false discovery rate.
    * Only applicable when selectorType = "fdr".
    * Default value is 0.05.
    *
    * @group param
    */

  final val fdr = new DoubleParam(this, "fdr",
    "The upper bound of the expected false discovery rate.", ParamValidators.inRange(0, 1))
  setDefault(fdr -> 0.05)

  /** @group getParam */
  def getFdr: Double = $(fdr)

  /**
    * The upper bound of the expected family-wise error rate.
    * Only applicable when selectorType = "fwe".
    * Default value is 0.05.
    *
    * @group param
    */

  final val fwe = new DoubleParam(this, "fwe",
    "The upper bound of the expected family-wise error rate.", ParamValidators.inRange(0, 1))
  setDefault(fwe -> 0.05)

  /** @group getParam */
  def getFwe: Double = $(fwe)

  /**
    * The selector type of the ChisqSelector.
    * Supported options: "numTopFeatures" (default), "percentile", "fpr", "fdr", "fwe".
    *
    * @group param
    */

  final val selectorType = new Param[String](this, "selectorType",
    "The selector type of the ChisqSelector. " +
      "Supported options: " + ChiSqSelector.supportedSelectorTypes.mkString(", "),
    ParamValidators.inArray[String](ChiSqSelector.supportedSelectorTypes))
  setDefault(selectorType -> ChiSqSelector.NumTopFeatures)

  /** @group getParam */

  def getSelectorType: String = $(selectorType)
}

/**
  * Chi-Squared feature selection, which selects categorical features to use for predicting a
  * categorical label.
  * The selector supports different selection methods: `numTopFeatures`, `percentile`, `fpr`,
  * `fdr`, `fwe`.
  *  - `numTopFeatures` chooses a fixed number of top features according to a chi-squared test.
  *  - `percentile` is similar but chooses a fraction of all features instead of a fixed number.
  *  - `fpr` chooses all features whose p-value are below a threshold, thus controlling the false
  * positive rate of selection.
  *  - `fdr` uses the [Benjamini-Hochberg procedure]
  * (https://en.wikipedia.org/wiki/False_discovery_rate#Benjamini.E2.80.93Hochberg_procedure)
  * to choose all features whose false discovery rate is below a threshold.
  *  - `fwe` chooses all features whose p-values are below a threshold. The threshold is scaled by
  * 1/numFeatures, thus controlling the family-wise error rate of selection.
  * By default, the selection method is `numTopFeatures`, with the default number of top features
  * set to 50.
  */

final class ChiSqSelector(override val uid: String)
  extends Estimator[ChiSqSelectorModel] with ChiSqSelectorParams with DefaultParamsWritable {


  def this() = this(Identifiable.randomUID("chiSqSelector"))

  /** @group setParam */

  def setNumTopFeatures(value: Int): this.type = set(numTopFeatures, value)

  /** @group setParam */

  def setPercentile(value: Double): this.type = set(percentile, value)

  /** @group setParam */

  def setFpr(value: Double): this.type = set(fpr, value)

  /** @group setParam */

  def setFdr(value: Double): this.type = set(fdr, value)

  /** @group setParam */

  def setFwe(value: Double): this.type = set(fwe, value)

  /** @group setParam */

  def setSelectorType(value: String): this.type = set(selectorType, value)

  /** @group setParam */

  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  /** @group setParam */

  def setOutputCol(value: String): this.type = set(outputCol, value)

  /** @group setParam */

  def setLabelCol(value: String): this.type = set(labelCol, value)


  override def fit(dataset: Dataset[_]): ChiSqSelectorModel = {
    transformSchema(dataset.schema, logging = true)
    val input: RDD[LabeledPoint] =
      dataset.select(col($(labelCol)).cast(DoubleType), col($(featuresCol))).rdd.map {
        case Row(label: Double, features: linalg.Vector) =>
          ml.feature.LabeledPoint(label, features)
      }

    val chiSqTestResult = Statistics.chiSqTest(input).zipWithIndex
    val features = $(selectorType) match {
      case ChiSqSelector.NumTopFeatures =>
        chiSqTestResult
          .sortBy { case (res, _) => res.pValue }
          .take($(numTopFeatures))
      case ChiSqSelector.Percentile =>
        chiSqTestResult
          .sortBy { case (res, _) => res.pValue }
          .take((chiSqTestResult.length * $(percentile)).toInt)
      case ChiSqSelector.FPR =>
        chiSqTestResult.filter { case (res, _) => res.pValue < $(fpr) }
      case ChiSqSelector.FDR =>
        // This uses the Benjamini-Hochberg procedure.
        // https://en.wikipedia.org/wiki/False_discovery_rate#Benjamini.E2.80.93Hochberg_procedure
        val tempRes = chiSqTestResult
          .sortBy { case (res, _) => res.pValue }
        val selected = tempRes
          .zipWithIndex
          .filter { case ((res, _), index) =>
            res.pValue <= $(fdr) * (index + 1) / chiSqTestResult.length
          }
        if (selected.isEmpty) {
          Array.empty[(ChiSqTestResult, Int)]
        } else {
          val maxIndex = selected.map(_._2).max
          tempRes.take(maxIndex + 1)
        }
      case ChiSqSelector.FWE =>
        chiSqTestResult
          .filter { case (res, _) => res.pValue < $ {
            fwe
          } / chiSqTestResult.length
          }
      case errorType =>
        throw new IllegalStateException(s"Unknown ChiSqSelector Type: $errorType")
    }
    val indices = features.map { case (_, index) => index }

    copyValues(new ChiSqSelectorModel(uid, indices).setParent(this))
  }


  override def transformSchema(schema: StructType): StructType = {
    val otherPairs = ChiSqSelector.supportedSelectorTypes.filter(_ != $(selectorType))
    otherPairs.foreach { paramName: String =>
      if (isSet(getParam(paramName))) {
        logWarning(s"Param $paramName will take no effect when selector type = ${$(selectorType)}.")
      }
    }
    SchemaUtils.checkColumnType(schema, $(featuresCol), new VectorUDT)
    SchemaUtils.checkNumericType(schema, $(labelCol))
    SchemaUtils.appendColumn(schema, $(outputCol), new VectorUDT)
  }


  override def copy(extra: ParamMap): ChiSqSelector = defaultCopy(extra)
}


object ChiSqSelector extends DefaultParamsReadable[ChiSqSelector] {


  override def load(path: String): ChiSqSelector = super.load(path)

  /** String name for `numTopFeatures` selector type. */
  val NumTopFeatures: String = "numTopFeatures"

  /** String name for `percentile` selector type. */
  val Percentile: String = "percentile"

  /** String name for `fpr` selector type. */
  val FPR: String = "fpr"

  /** String name for `fdr` selector type. */
  val FDR: String = "fdr"

  /** String name for `fwe` selector type. */
  val FWE: String = "fwe"

  val supportedSelectorTypes: Array[String] = Array(NumTopFeatures, Percentile, FPR, FDR, FWE)
}

/**
  * Model fitted by [[ChiSqSelector]].
  */

final class ChiSqSelectorModel private[angel](
                                               override val uid: String,
                                               val selectedFeatures: Array[Int])
  extends Model[ChiSqSelectorModel] with ChiSqSelectorParams with MLWritable {

  import ChiSqSelectorModel._

  /** @group setParam */

  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  /** @group setParam */

  def setOutputCol(value: String): this.type = set(outputCol, value)


  override def transform(dataset: Dataset[_]): DataFrame = {
    val transformedSchema = transformSchema(dataset.schema, logging = true)
    val newField = transformedSchema.last

    // TODO: Make the transformer natively in ml framework to avoid extra conversion.
    val transformer: linalg.Vector => linalg.Vector = v => compress(v)

    val selector = udf(transformer)
    DatasetUtil.withColumn(dataset, $(outputCol), selector(col($(featuresCol))), newField.metadata)
  }


  override def transformSchema(schema: StructType): StructType = {
    SchemaUtils.checkColumnType(schema, $(featuresCol), new VectorUDT)
    val newField = prepOutputField(schema)
    val outputFields = schema.fields :+ newField
    StructType(outputFields)
  }

  /**
    * Prepare the output column field, including per-feature metadata.
    */
  private def prepOutputField(schema: StructType): StructField = {
    val selector = selectedFeatures.toSet
    val origAttrGroup = AttributeGroup.fromStructField(schema($(featuresCol)))
    val featureAttributes: Array[Attribute] = if (origAttrGroup.attributes.nonEmpty) {
      origAttrGroup.attributes.get.zipWithIndex.filter(x => selector.contains(x._2)).map(_._1)
    } else {
      Array.fill[Attribute](selector.size)(NominalAttribute.defaultAttr)
    }
    val newAttributeGroup = new AttributeGroup($(outputCol), featureAttributes)
    newAttributeGroup.toStructField
  }


  override def copy(extra: ParamMap): ChiSqSelectorModel = {
    val copied = new ChiSqSelectorModel(uid, selectedFeatures)
    copyValues(copied, extra).setParent(parent)
  }


  override def write: MLWriter = new ChiSqSelectorModelWriter(this)

  private val filterIndices = selectedFeatures.sorted

  protected def formatVersion: String = "1.0"

  /**
    * Returns a vector with features filtered.
    * Preserves the order of filtered features the same as their indices are stored.
    * Might be moved to Vector as .slice
    *
    * @param features vector
    */
  private def compress(features: linalg.Vector): linalg.Vector = {
    features match {
      case IntSparseVector(_, indices, values) =>
        val newSize = filterIndices.length
        val newValues = new mutable.ArrayBuilder.ofDouble
        val newIndices = new mutable.ArrayBuilder.ofInt
        var i = 0
        var j = 0
        var indicesIdx = 0
        var filterIndicesIdx = 0
        while (i < indices.length && j < filterIndices.length) {
          indicesIdx = indices(i)
          filterIndicesIdx = filterIndices(j)
          if (indicesIdx == filterIndicesIdx) {
            newIndices += j
            newValues += values(i)
            j += 1
            i += 1
          } else {
            if (indicesIdx > filterIndicesIdx) {
              j += 1
            } else {
              i += 1
            }
          }
        }
        // TODO: Sparse representation might be ineffective if (newSize ~= newValues.size)
        Vectors.sparse(newSize, newIndices.result(), newValues.result())
      case LongSparseVector(_, indices, values) =>
        val newSize = filterIndices.length
        val newValues = new mutable.ArrayBuilder.ofDouble
        val newIndices = new mutable.ArrayBuilder.ofLong
        var i = 0
        var j = 0
        var indicesIdx = 0L
        var filterIndicesIdx = 0
        while (i < indices.length && j < filterIndices.length) {
          indicesIdx = indices(i)
          filterIndicesIdx = filterIndices(j)
          if (indicesIdx == filterIndicesIdx) {
            newIndices += j
            newValues += values(i)
            j += 1
            i += 1
          } else {
            if (indicesIdx > filterIndicesIdx) {
              j += 1
            } else {
              i += 1
            }
          }
        }
        // TODO: Sparse representation might be ineffective if (newSize ~= newValues.size)
        Vectors.sparse(newSize, newIndices.result(), newValues.result())
      case DenseVector(_) =>
        val values = features.toArray
        Vectors.dense(filterIndices.map(i => values(i)))
      case other =>
        throw new UnsupportedOperationException(
          s"Only sparse and dense vectors are supported but got ${other.getClass}.")
    }
  }
}


object ChiSqSelectorModel extends MLReadable[ChiSqSelectorModel] {

  private[ChiSqSelectorModel]
  class ChiSqSelectorModelWriter(instance: ChiSqSelectorModel) extends MLWriter {

    private case class Data(selectedFeatures: Seq[Int])

    override protected def saveImpl(path: String): Unit = {
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val data = Data(instance.selectedFeatures.toSeq)
      val dataPath = new Path(path, "data").toString
      sparkSession.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
    }
  }

  private class ChiSqSelectorModelReader extends MLReader[ChiSqSelectorModel] {

    private val className = classOf[ChiSqSelectorModel].getName

    override def load(path: String): ChiSqSelectorModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val dataPath = new Path(path, "data").toString
      val data = sparkSession.read.parquet(dataPath).select("selectedFeatures").head()
      val selectedFeatures = data.getAs[Seq[Int]](0).toArray
      val model = new ChiSqSelectorModel(metadata.uid, selectedFeatures)
      metadata.getAndSetParams(model)
      model
    }
  }


  override def read: MLReader[ChiSqSelectorModel] = new ChiSqSelectorModelReader


  override def load(path: String): ChiSqSelectorModel = super.load(path)
}
