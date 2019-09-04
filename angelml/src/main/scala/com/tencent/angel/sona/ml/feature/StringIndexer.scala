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

import scala.language.existentials
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkException
import com.tencent.angel.sona.ml.{Estimator, Model, Transformer}
import com.tencent.angel.sona.ml.attribute.{Attribute, NominalAttribute}
import com.tencent.angel.sona.ml.param.{Param, ParamMap, ParamValidators, Params, StringArrayParam}
import com.tencent.angel.sona.ml.param.shared.{HasHandleInvalid, HasInputCol, HasOutputCol}
import com.tencent.angel.sona.ml.util._
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.util.OpenHashMap


/**
  * Base trait for [[StringIndexer]] and [[StringIndexerModel]].
  */
private[sona] trait StringIndexerBase extends Params with HasHandleInvalid with HasInputCol
  with HasOutputCol {

  /**
    * Param for how to handle invalid data (unseen labels or NULL values).
    * Options are 'skip' (filter out rows with invalid data),
    * 'error' (throw an error), or 'keep' (put invalid data in a special additional
    * bucket, at index numLabels).
    * Default: "error"
    *
    * @group param
    */

  override val handleInvalid: Param[String] = new Param[String](this, "handleInvalid",
    "How to handle invalid data (unseen labels or NULL values). " +
      "Options are 'skip' (filter out rows with invalid data), error (throw an error), " +
      "or 'keep' (put invalid data in a special additional bucket, at index numLabels).",
    ParamValidators.inArray(StringIndexer.supportedHandleInvalids))

  setDefault(handleInvalid, StringIndexer.ERROR_INVALID)

  /**
    * Param for how to order labels of string column. The first label after ordering is assigned
    * an index of 0.
    * Options are:
    *   - 'frequencyDesc': descending order by label frequency (most frequent label assigned 0)
    *   - 'frequencyAsc': ascending order by label frequency (least frequent label assigned 0)
    *   - 'alphabetDesc': descending alphabetical order
    *   - 'alphabetAsc': ascending alphabetical order
    * Default is 'frequencyDesc'.
    *
    * @group param
    */

  final val stringOrderType: Param[String] = new Param(this, "stringOrderType",
    "How to order labels of string column. " +
      "The first label after ordering is assigned an index of 0. " +
      s"Supported options: ${StringIndexer.supportedStringOrderType.mkString(", ")}.",
    ParamValidators.inArray(StringIndexer.supportedStringOrderType))

  /** @group getParam */

  def getStringOrderType: String = $(stringOrderType)

  /** Validates and transforms the input schema. */
  protected def validateAndTransformSchema(schema: StructType): StructType = {
    val inputColName = $(inputCol)
    val inputDataType = schema(inputColName).dataType
    require(inputDataType == StringType || inputDataType.isInstanceOf[NumericType],
      s"The input column $inputColName must be either string type or numeric type, " +
        s"but got $inputDataType.")
    val inputFields = schema.fields
    val outputColName = $(outputCol)
    require(inputFields.forall(_.name != outputColName),
      s"Output column $outputColName already exists.")
    val attr = NominalAttribute.defaultAttr.withName($(outputCol))
    val outputFields = inputFields :+ attr.toStructField()
    StructType(outputFields)
  }
}

/**
  * A label indexer that maps a string column of labels to an ML column of label indices.
  * If the input column is numeric, we cast it to string and index the string values.
  * The indices are in [0, numLabels). By default, this is ordered by label frequencies
  * so the most frequent label gets index 0. The ordering behavior is controlled by
  * setting `stringOrderType`.
  *
  * @see `IndexToString` for the inverse transformation
  */

class StringIndexer(override val uid: String) extends Estimator[StringIndexerModel]
  with StringIndexerBase with DefaultParamsWritable {


  def this() = this(Identifiable.randomUID("strIdx"))

  /** @group setParam */
  def setHandleInvalid(value: String): this.type = set(handleInvalid, value)

  /** @group setParam */
  def setStringOrderType(value: String): this.type = set(stringOrderType, value)

  setDefault(stringOrderType, StringIndexer.frequencyDesc)

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  override def fit(dataset: Dataset[_]): StringIndexerModel = {
    transformSchema(dataset.schema, logging = true)
    val values = dataset.na.drop(Array($(inputCol)))
      .select(col($(inputCol)).cast(StringType))
      .rdd.map(_.getString(0))
    val labels = $(stringOrderType) match {
      case StringIndexer.frequencyDesc => values.countByValue().toSeq.sortBy(-_._2)
        .map(_._1).toArray
      case StringIndexer.frequencyAsc => values.countByValue().toSeq.sortBy(_._2)
        .map(_._1).toArray
      case StringIndexer.alphabetDesc => values.distinct.collect.sortWith(_ > _)
      case StringIndexer.alphabetAsc => values.distinct.collect.sortWith(_ < _)
    }
    copyValues(new StringIndexerModel(uid, labels).setParent(this))
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }


  override def copy(extra: ParamMap): StringIndexer = defaultCopy(extra)
}


object StringIndexer extends DefaultParamsReadable[StringIndexer] {
  private[sona] val SKIP_INVALID: String = "skip"
  private[sona] val ERROR_INVALID: String = "error"
  private[sona] val KEEP_INVALID: String = "keep"
  private[sona] val supportedHandleInvalids: Array[String] =
    Array(SKIP_INVALID, ERROR_INVALID, KEEP_INVALID)
  private[sona] val frequencyDesc: String = "frequencyDesc"
  private[sona] val frequencyAsc: String = "frequencyAsc"
  private[sona] val alphabetDesc: String = "alphabetDesc"
  private[sona] val alphabetAsc: String = "alphabetAsc"
  private[sona] val supportedStringOrderType: Array[String] =
    Array(frequencyDesc, frequencyAsc, alphabetDesc, alphabetAsc)


  override def load(path: String): StringIndexer = super.load(path)
}

/**
  * Model fitted by [[StringIndexer]].
  *
  * @param labels Ordered list of labels, corresponding to indices to be assigned.
  * @note During transformation, if the input column does not exist,
  *       `StringIndexerModel.transform` would return the input dataset unmodified.
  *       This is a temporary fix for the case when target labels do not exist during prediction.
  */

class StringIndexerModel(override val uid: String, val labels: Array[String])
  extends Model[StringIndexerModel] with StringIndexerBase with MLWritable {

  import StringIndexerModel._

  def this(labels: Array[String]) = this(Identifiable.randomUID("strIdx"), labels)

  private val labelToIndex: OpenHashMap[String, Double] = {
    val n = labels.length
    val map = new OpenHashMap[String, Double](n)
    var i = 0
    while (i < n) {
      map.update(labels(i), i)
      i += 1
    }
    map
  }

  /** @group setParam */
  def setHandleInvalid(value: String): this.type = set(handleInvalid, value)

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  override def transform(dataset: Dataset[_]): DataFrame = {
    if (!dataset.schema.fieldNames.contains($(inputCol))) {
      logInfo(s"Input column ${$(inputCol)} does not exist during transformation. " +
        "Skip StringIndexerModel.")
      return dataset.toDF
    }
    transformSchema(dataset.schema, logging = true)

    val filteredLabels = getHandleInvalid match {
      case StringIndexer.KEEP_INVALID => labels :+ "__unknown"
      case _ => labels
    }

    val metadata = NominalAttribute.defaultAttr
      .withName($(outputCol)).withValues(filteredLabels).toMetadata()
    // If we are skipping invalid records, filter them out.
    val (filteredDataset, keepInvalid) = $(handleInvalid) match {
      case StringIndexer.SKIP_INVALID =>
        val filterer = udf { label: String =>
          labelToIndex.contains(label)
        }
        (dataset.na.drop(Array($(inputCol))).where(filterer(dataset($(inputCol)))), false)
      case _ => (dataset, getHandleInvalid == StringIndexer.KEEP_INVALID)
    }

    val indexer = udf { label: String =>
      if (label == null) {
        if (keepInvalid) {
          labels.length
        } else {
          throw new SparkException("StringIndexer encountered NULL value. To handle or skip " +
            "NULLS, try setting StringIndexer.handleInvalid.")
        }
      } else {
        if (labelToIndex.contains(label)) {
          labelToIndex(label)
        } else if (keepInvalid) {
          labels.length
        } else {
          throw new SparkException(s"Unseen label: $label.  To handle unseen labels, " +
            s"set Param handleInvalid to ${StringIndexer.KEEP_INVALID}.")
        }
      }
    }

    filteredDataset.select(col("*"),
      indexer(dataset($(inputCol)).cast(StringType)).as($(outputCol), metadata))
  }

  override def transformSchema(schema: StructType): StructType = {
    if (schema.fieldNames.contains($(inputCol))) {
      validateAndTransformSchema(schema)
    } else {
      // If the input column does not exist during transformation, we skip StringIndexerModel.
      schema
    }
  }

  override def copy(extra: ParamMap): StringIndexerModel = {
    val copied = new StringIndexerModel(uid, labels)
    copyValues(copied, extra).setParent(parent)
  }

  override def write: StringIndexModelWriter = new StringIndexModelWriter(this)
}


object StringIndexerModel extends MLReadable[StringIndexerModel] {

  private[StringIndexerModel]
  class StringIndexModelWriter(instance: StringIndexerModel) extends MLWriter {

    private case class Data(labels: Array[String])

    override protected def saveImpl(path: String): Unit = {
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val data = Data(instance.labels)
      val dataPath = new Path(path, "data").toString
      sparkSession.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
    }
  }

  private class StringIndexerModelReader extends MLReader[StringIndexerModel] {

    private val className = classOf[StringIndexerModel].getName

    override def load(path: String): StringIndexerModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val dataPath = new Path(path, "data").toString
      val data = sparkSession.read.parquet(dataPath)
        .select("labels")
        .head()
      val labels = data.getAs[Seq[String]](0).toArray
      val model = new StringIndexerModel(metadata.uid, labels)
      metadata.getAndSetParams(model)
      model
    }
  }

  override def read: MLReader[StringIndexerModel] = new StringIndexerModelReader

  override def load(path: String): StringIndexerModel = super.load(path)
}

/**
  * A `Transformer` that maps a column of indices back to a new column of corresponding
  * string values.
  * The index-string mapping is either from the ML attributes of the input column,
  * or from user-supplied labels (which take precedence over ML attributes).
  *
  * @see `StringIndexer` for converting strings into indices
  */
class IndexToString(override val uid: String)
  extends Transformer with HasInputCol with HasOutputCol with DefaultParamsWritable {

  def this() =
    this(Identifiable.randomUID("idxToStr"))

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  /** @group setParam */
  def setLabels(value: Array[String]): this.type = set(labels, value)

  /**
    * Optional param for array of labels specifying index-string mapping.
    *
    * Default: Not specified, in which case [[inputCol]] metadata is used for labels.
    *
    * @group param
    */
  final val labels: StringArrayParam = new StringArrayParam(this, "labels",
    "Optional array of labels specifying index-string mapping." +
      " If not provided or if empty, then metadata from inputCol is used instead.")

  /** @group getParam */
  final def getLabels: Array[String] = $(labels)

  override def transformSchema(schema: StructType): StructType = {
    val inputColName = $(inputCol)
    val inputDataType = schema(inputColName).dataType
    require(inputDataType.isInstanceOf[NumericType],
      s"The input column $inputColName must be a numeric type, " +
        s"but got $inputDataType.")
    val inputFields = schema.fields
    val outputColName = $(outputCol)
    require(inputFields.forall(_.name != outputColName),
      s"Output column $outputColName already exists.")
    val outputFields = inputFields :+ StructField($(outputCol), StringType)
    StructType(outputFields)
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema, logging = true)
    val inputColSchema = dataset.schema($(inputCol))
    // If the labels array is empty use column metadata
    val values = if (!isDefined(labels) || $(labels).isEmpty) {
      Attribute.fromStructField(inputColSchema)
        .asInstanceOf[NominalAttribute].values.get
    } else {
      $(labels)
    }
    val indexer = udf { index: Double =>
      val idx = index.toInt
      if (0 <= idx && idx < values.length) {
        values(idx)
      } else {
        throw new SparkException(s"Unseen index: $index ??")
      }
    }
    val outputColName = $(outputCol)
    dataset.select(col("*"),
      indexer(dataset($(inputCol)).cast(DoubleType)).as(outputColName))
  }

  override def copy(extra: ParamMap): IndexToString = {
    defaultCopy(extra)
  }
}


object IndexToString extends DefaultParamsReadable[IndexToString] {
  override def load(path: String): IndexToString = super.load(path)
}
