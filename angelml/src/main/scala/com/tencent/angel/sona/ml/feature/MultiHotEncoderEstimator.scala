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
import org.apache.spark.SparkException
import com.tencent.angel.sona.ml.{Estimator, Model}
import com.tencent.angel.sona.ml.attribute.{Attribute, AttributeGroup, BinaryAttribute, NominalAttribute, NumericAttribute}
import org.apache.spark.linalg.Vectors
import com.tencent.angel.sona.ml.param.{BooleanParam, Param, ParamMap, ParamValidators, Params}
import com.tencent.angel.sona.ml.param.shared.{HasHandleInvalid, HasInputCols, HasOutputCols}
import com.tencent.angel.sona.ml.util._
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, lit, udf}
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.util.SONASchemaUtils


/** Private trait for params and common methods for MultiHotEncoderEstimator and MultiHotEncoderModel */
private[angel] trait MultiHotEncoderBase extends Params with HasHandleInvalid
  with HasInputCols with HasOutputCols {

  /**
    * Param for how to handle invalid data during transform().
    * Options are 'keep' (invalid data presented as an extra categorical feature) or
    * 'error' (throw an error).
    * Note that this Param is only used during transform; during fitting, invalid data
    * will result in an error.
    * Default: "error"
    *
    * @group param
    */

  override val handleInvalid: Param[String] = new Param[String](this, "handleInvalid",
    "How to handle invalid data during transform(). " +
      "Options are 'keep' (invalid data presented as an extra categorical feature) " +
      "or error (throw an error). Note that this Param is only used during transform; " +
      "during fitting, invalid data will result in an error.",
    ParamValidators.inArray(MultiHotEncoderEstimator.supportedHandleInvalids))

  setDefault(handleInvalid, MultiHotEncoderEstimator.ERROR_INVALID)

  /**
    * Whether to drop the last category in the encoded vector (default: true)
    *
    * @group param
    */

  final val dropLast: BooleanParam =
    new BooleanParam(this, "dropLast", "whether to drop the last category")
  setDefault(dropLast -> true)

  /** @group getParam */

  def getDropLast: Boolean = $(dropLast)

  protected def validateAndTransformSchema(
                                            schema: StructType,
                                            dropLast: Boolean,
                                            keepInvalid: Boolean): StructType = {
    val inputColNames = $(inputCols)
    val outputColNames = $(outputCols)

    require(inputColNames.length == outputColNames.length,
      s"The number of input columns ${inputColNames.length} must be the same as the number of " +
        s"output columns ${outputColNames.length}.")

    // Input columns must be NumericType.
    inputColNames.foreach(SONASchemaUtils.checkNumericType(schema, _))

    // Prepares output columns with proper attributes by examining input columns.
    val inputFields = $(inputCols).map(schema(_))

    val outputFields = inputFields.zip(outputColNames).map { case (inputField, outputColName) =>
      MultiHotEncoderCommon.transformOutputColumnSchema(
        inputField, outputColName, dropLast, keepInvalid)
    }
    outputFields.foldLeft(schema) { case (newSchema, outputField) =>
      SONASchemaUtils.appendColumn(newSchema, outputField)
    }
  }
}

/**
  * A one-hot encoder that maps a column of category indices to a column of binary vectors, with
  * at most a single one-value per row that indicates the input category index.
  * For example with 5 categories, an input value of 2.0 would map to an output vector of
  * `[0.0, 0.0, 1.0, 0.0]`.
  * The last category is not included by default (configurable via `dropLast`),
  * because it makes the vector entries sum up to one, and hence linearly dependent.
  * So an input value of 4.0 maps to `[0.0, 0.0, 0.0, 0.0]`.
  *
  * @note This is different from scikit-learn's MultiHotEncoder, which keeps all categories.
  *       The output vectors are sparse.
  *
  *       When `handleInvalid` is configured to 'keep', an extra "category" indicating invalid values is
  *       added as last category. So when `dropLast` is true, invalid values are encoded as all-zeros
  *       vector.
  * @note When encoding multi-column by using `inputCols` and `outputCols` params, input/output cols
  *       come in pairs, specified by the order in the arrays, and each pair is treated independently.
  * @see `StringIndexer` for converting categorical values into category indices
  */

class MultiHotEncoderEstimator(override val uid: String)
  extends Estimator[MultiHotEncoderModel] with MultiHotEncoderBase with DefaultParamsWritable {


  def this() = this(Identifiable.randomUID("oneHotEncoder"))

  /** @group setParam */

  def setInputCols(values: Array[String]): this.type = set(inputCols, values)

  /** @group setParam */

  def setOutputCols(values: Array[String]): this.type = set(outputCols, values)

  /** @group setParam */

  def setDropLast(value: Boolean): this.type = set(dropLast, value)

  /** @group setParam */

  def setHandleInvalid(value: String): this.type = set(handleInvalid, value)


  override def transformSchema(schema: StructType): StructType = {
    val keepInvalid = $(handleInvalid) == MultiHotEncoderEstimator.KEEP_INVALID
    validateAndTransformSchema(schema, dropLast = $(dropLast),
      keepInvalid = keepInvalid)
  }


  override def fit(dataset: Dataset[_]): MultiHotEncoderModel = {
    transformSchema(dataset.schema)

    // Compute the plain number of categories without `handleInvalid` and
    // `dropLast` taken into account.
    val transformedSchema = validateAndTransformSchema(dataset.schema, dropLast = false,
      keepInvalid = false)
    val categorySizes = new Array[Int]($(outputCols).length)

    val columnToScanIndices = $(outputCols).zipWithIndex.flatMap { case (outputColName, idx) =>
      val numOfAttrs = AttributeGroup.fromStructField(
        transformedSchema(outputColName)).size.toInt
      if (numOfAttrs < 0) {
        Some(idx)
      } else {
        categorySizes(idx) = numOfAttrs
        None
      }
    }

    // Some input columns don't have attributes or their attributes don't have necessary info.
    // We need to scan the data to get the number of values for each column.
    if (columnToScanIndices.length > 0) {
      val inputColNames = columnToScanIndices.map($(inputCols)(_))
      val outputColNames = columnToScanIndices.map($(outputCols)(_))

      // When fitting data, we want the plain number of categories without `handleInvalid` and
      // `dropLast` taken into account.
      val attrGroups = MultiHotEncoderCommon.getOutputAttrGroupFromData(
        dataset, inputColNames, outputColNames, dropLast = false)
      attrGroups.zip(columnToScanIndices).foreach { case (attrGroup, idx) =>
        categorySizes(idx) = attrGroup.size.toInt
      }
    }

    val model = new MultiHotEncoderModel(uid, categorySizes).setParent(this)
    copyValues(model)
  }


  override def copy(extra: ParamMap): MultiHotEncoderEstimator = defaultCopy(extra)
}


object MultiHotEncoderEstimator extends DefaultParamsReadable[MultiHotEncoderEstimator] {

  private[sona] val KEEP_INVALID: String = "keep"
  private[sona] val ERROR_INVALID: String = "error"
  private[sona] val supportedHandleInvalids: Array[String] = Array(KEEP_INVALID, ERROR_INVALID)

  override def load(path: String): MultiHotEncoderEstimator = super.load(path)
}

/**
  * @param categorySizes Original number of categories for each feature being encoded.
  *                      The array contains one value for each input column, in order.
  */

class MultiHotEncoderModel private[angel](
                                           override val uid: String,
                                           val categorySizes: Array[Int])
  extends Model[MultiHotEncoderModel] with MultiHotEncoderBase with MLWritable {

  import MultiHotEncoderModel._

  // Returns the category size for each index with `dropLast` and `handleInvalid`
  // taken into account.
  private def getConfigedCategorySizes: Array[Int] = {
    val dropLast = getDropLast
    val keepInvalid = getHandleInvalid == MultiHotEncoderEstimator.KEEP_INVALID

    if (!dropLast && keepInvalid) {
      // When `handleInvalid` is "keep", an extra category is added as last category
      // for invalid data.
      categorySizes.map(_ + 1)
    } else if (dropLast && !keepInvalid) {
      // When `dropLast` is true, the last category is removed.
      categorySizes.map(_ - 1)
    } else {
      // When `dropLast` is true and `handleInvalid` is "keep", the extra category for invalid
      // data is removed. Thus, it is the same as the plain number of categories.
      categorySizes
    }
  }

  private def encoder: UserDefinedFunction = {
    val keepInvalid = getHandleInvalid == MultiHotEncoderEstimator.KEEP_INVALID
    val configedSizes = getConfigedCategorySizes
    val localCategorySizes = categorySizes

    // The udf performed on input data. The first parameter is the input value. The second
    // parameter is the index in inputCols of the column being encoded.
    udf { (label: Double, colIdx: Int) =>
      val origCategorySize = localCategorySizes(colIdx)
      // idx: index in vector of the single 1-valued element
      val idx = if (label >= 0 && label < origCategorySize) {
        label
      } else {
        if (keepInvalid) {
          origCategorySize
        } else {
          if (label < 0) {
            throw new SparkException(s"Negative value: $label. Input can't be negative. " +
              s"To handle invalid values, set Param handleInvalid to " +
              s"${MultiHotEncoderEstimator.KEEP_INVALID}")
          } else {
            throw new SparkException(s"Unseen value: $label. To handle unseen values, " +
              s"set Param handleInvalid to ${MultiHotEncoderEstimator.KEEP_INVALID}.")
          }
        }
      }

      val size = configedSizes(colIdx)
      if (idx < size) {
        Vectors.sparse(size, Array(idx.toInt), Array(1.0))
      } else {
        Vectors.sparse(size, Array.empty[Int], Array.empty[Double])
      }
    }
  }

  /** @group setParam */

  def setInputCols(values: Array[String]): this.type = set(inputCols, values)

  /** @group setParam */

  def setOutputCols(values: Array[String]): this.type = set(outputCols, values)

  /** @group setParam */

  def setDropLast(value: Boolean): this.type = set(dropLast, value)

  /** @group setParam */

  def setHandleInvalid(value: String): this.type = set(handleInvalid, value)


  override def transformSchema(schema: StructType): StructType = {
    val inputColNames = $(inputCols)

    require(inputColNames.length == categorySizes.length,
      s"The number of input columns ${inputColNames.length} must be the same as the number of " +
        s"features ${categorySizes.length} during fitting.")

    val keepInvalid = $(handleInvalid) == MultiHotEncoderEstimator.KEEP_INVALID
    val transformedSchema = validateAndTransformSchema(schema, dropLast = $(dropLast),
      keepInvalid = keepInvalid)
    verifyNumOfValues(transformedSchema)
  }

  /**
    * If the metadata of input columns also specifies the number of categories, we need to
    * compare with expected category number with `handleInvalid` and `dropLast` taken into
    * account. Mismatched numbers will cause exception.
    */
  private def verifyNumOfValues(schema: StructType): StructType = {
    val configedSizes = getConfigedCategorySizes
    $(outputCols).zipWithIndex.foreach { case (outputColName, idx) =>
      val inputColName = $(inputCols)(idx)
      val attrGroup = AttributeGroup.fromStructField(schema(outputColName))

      // If the input metadata specifies number of category for output column,
      // comparing with expected category number with `handleInvalid` and
      // `dropLast` taken into account.
      if (attrGroup.attributes.nonEmpty) {
        val numCategories = configedSizes(idx)
        require(attrGroup.size == numCategories, "MultiHotEncoderModel expected " +
          s"$numCategories categorical values for input column $inputColName, " +
          s"but the input column had metadata specifying ${attrGroup.size} values.")
      }
    }
    schema
  }


  override def transform(dataset: Dataset[_]): DataFrame = {
    val transformedSchema = transformSchema(dataset.schema, logging = true)
    val keepInvalid = $(handleInvalid) == MultiHotEncoderEstimator.KEEP_INVALID

    val encodedColumns = $(inputCols).indices.map { idx =>
      val inputColName = $(inputCols)(idx)
      val outputColName = $(outputCols)(idx)

      val outputAttrGroupFromSchema =
        AttributeGroup.fromStructField(transformedSchema(outputColName))

      val metadata = if (outputAttrGroupFromSchema.size < 0) {
        MultiHotEncoderCommon.createAttrGroupForAttrNames(outputColName,
          categorySizes(idx), $(dropLast), keepInvalid).toMetadata
      } else {
        outputAttrGroupFromSchema.toMetadata
      }

      encoder(col(inputColName).cast(DoubleType), lit(idx))
        .as(outputColName, metadata)
    }
    var finalDataset = dataset
    encodedColumns.indices.foreach { index =>
      finalDataset = finalDataset.withColumn($(outputCols)(index), encodedColumns(index))
    }
    finalDataset.toDF()
    //    dataset.withColumns($(outputCols), encodedColumns)
  }


  override def copy(extra: ParamMap): MultiHotEncoderModel = {
    val copied = new MultiHotEncoderModel(uid, categorySizes)
    copyValues(copied, extra).setParent(parent)
  }


  override def write: MLWriter = new MultiHotEncoderModelWriter(this)
}


object MultiHotEncoderModel extends MLReadable[MultiHotEncoderModel] {

  private[MultiHotEncoderModel]
  class MultiHotEncoderModelWriter(instance: MultiHotEncoderModel) extends MLWriter {

    private case class Data(categorySizes: Array[Int])

    override protected def saveImpl(path: String): Unit = {
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val data = Data(instance.categorySizes)
      val dataPath = new Path(path, "data").toString
      sparkSession.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
    }
  }

  private class MultiHotEncoderModelReader extends MLReader[MultiHotEncoderModel] {

    private val className = classOf[MultiHotEncoderModel].getName

    override def load(path: String): MultiHotEncoderModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val dataPath = new Path(path, "data").toString
      val data = sparkSession.read.parquet(dataPath)
        .select("categorySizes")
        .head()
      val categorySizes = data.getAs[Seq[Int]](0).toArray
      val model = new MultiHotEncoderModel(metadata.uid, categorySizes)
      metadata.getAndSetParams(model)
      model
    }
  }


  override def read: MLReader[MultiHotEncoderModel] = new MultiHotEncoderModelReader


  override def load(path: String): MultiHotEncoderModel = super.load(path)
}

/**
  * Provides some helper methods used by both `MultiHotEncoder` and `MultiHotEncoderEstimator`.
  */
private[sona] object MultiHotEncoderCommon {

  private def genOutputAttrNames(inputCol: StructField): Option[Array[String]] = {
    val inputAttr = Attribute.fromStructField(inputCol)
    inputAttr match {
      case nominal: NominalAttribute =>
        if (nominal.values.isDefined) {
          nominal.values
        } else if (nominal.numValues.isDefined) {
          nominal.numValues.map(n => Array.tabulate(n)(_.toString))
        } else {
          None
        }
      case binary: BinaryAttribute =>
        if (binary.values.isDefined) {
          binary.values
        } else {
          Some(Array.tabulate(2)(_.toString))
        }
      case _: NumericAttribute =>
        throw new RuntimeException(
          s"The input column ${inputCol.name} cannot be continuous-value.")
      case _ =>
        None // optimistic about unknown attributes
    }
  }

  /** Creates an `AttributeGroup` filled by the `BinaryAttribute` named as required. */
  private def genOutputAttrGroup(
                                  outputAttrNames: Option[Array[String]],
                                  outputColName: String): AttributeGroup = {
    outputAttrNames.map { attrNames =>
      val attrs: Array[Attribute] = attrNames.map { name =>
        BinaryAttribute.defaultAttr.withName(name)
      }
      new AttributeGroup(outputColName, attrs)
    }.getOrElse {
      new AttributeGroup(outputColName)
    }
  }

  /**
    * Prepares the `StructField` with proper metadata for `MultiHotEncoder`'s output column.
    */
  def transformOutputColumnSchema(
                                   inputCol: StructField,
                                   outputColName: String,
                                   dropLast: Boolean,
                                   keepInvalid: Boolean = false): StructField = {
    val outputAttrNames = genOutputAttrNames(inputCol)
    val filteredOutputAttrNames = outputAttrNames.map { names =>
      if (dropLast && !keepInvalid) {
        require(names.length > 1,
          s"The input column ${inputCol.name} should have at least two distinct values.")
        names.dropRight(1)
      } else if (!dropLast && keepInvalid) {
        names ++ Seq("invalidValues")
      } else {
        names
      }
    }

    genOutputAttrGroup(filteredOutputAttrNames, outputColName).toStructField
  }

  /**
    * This method is called when we want to generate `AttributeGroup` from actual data for
    * one-hot encoder.
    */
  def getOutputAttrGroupFromData(
                                  dataset: Dataset[_],
                                  inputColNames: Seq[String],
                                  outputColNames: Seq[String],
                                  dropLast: Boolean): Seq[AttributeGroup] = {
    // The RDD approach has advantage of early-stop if any values are invalid. It seems that
    // DataFrame ops don't have equivalent functions.
    val columns = inputColNames.map { inputColName =>
      col(inputColName).cast(DoubleType)
    }
    val numOfColumns = columns.length

    val numAttrsArray = dataset.select(columns: _*).rdd.map { row =>
      (0 until numOfColumns).map(idx => row.getDouble(idx)).toArray
    }.treeAggregate(new Array[Double](numOfColumns))(
      (maxValues, curValues) => {
        (0 until numOfColumns).foreach { idx =>
          val x = curValues(idx)
          assert(x <= Int.MaxValue,
            s"MultiHotEncoder only supports up to ${Int.MaxValue} indices, but got $x.")
          assert(x >= 0.0 && x == x.toInt,
            s"Values from column ${inputColNames(idx)} must be indices, but got $x.")
          maxValues(idx) = math.max(maxValues(idx), x)
        }
        maxValues
      },
      (m0, m1) => {
        (0 until numOfColumns).foreach { idx =>
          m0(idx) = math.max(m0(idx), m1(idx))
        }
        m0
      }
    ).map(_.toInt + 1)

    outputColNames.zip(numAttrsArray).map { case (outputColName, numAttrs) =>
      createAttrGroupForAttrNames(outputColName, numAttrs, dropLast, keepInvalid = false)
    }
  }

  /** Creates an `AttributeGroup` with the required number of `BinaryAttribute`. */
  def createAttrGroupForAttrNames(
                                   outputColName: String,
                                   numAttrs: Int,
                                   dropLast: Boolean,
                                   keepInvalid: Boolean): AttributeGroup = {
    val outputAttrNames = Array.tabulate(numAttrs)(_.toString)
    val filtered = if (dropLast && !keepInvalid) {
      outputAttrNames.dropRight(1)
    } else if (!dropLast && keepInvalid) {
      outputAttrNames ++ Seq("invalidValues")
    } else {
      outputAttrNames
    }
    genOutputAttrGroup(Some(filtered), outputColName)
  }
}
