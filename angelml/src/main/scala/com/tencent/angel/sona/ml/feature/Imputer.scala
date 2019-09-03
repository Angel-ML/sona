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
import com.tencent.angel.sona.ml.param.{DoubleParam, Param, ParamMap, ParamValidators, Params}
import com.tencent.angel.sona.ml.param.shared.{HasInputCols, HasOutputCols}
import com.tencent.angel.sona.ml.util._
import org.apache.spark.sql.util.SchemaUtils
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
  * Params for [[Imputer]] and [[ImputerModel]].
  */
private[sona] trait ImputerParams extends Params with HasInputCols with HasOutputCols {

  /**
    * The imputation strategy. Currently only "mean" and "median" are supported.
    * If "mean", then replace missing values using the mean value of the feature.
    * If "median", then replace missing values using the approximate median value of the feature.
    * Default: mean
    *
    * @group param
    */
  final val strategy: Param[String] = new Param(this, "strategy", s"strategy for imputation. " +
    s"If ${Imputer.mean}, then replace missing values using the mean value of the feature. " +
    s"If ${Imputer.median}, then replace missing values using the median value of the feature.",
    ParamValidators.inArray[String](Array(Imputer.mean, Imputer.median)))

  /** @group getParam */
  def getStrategy: String = $(strategy)

  /**
    * The placeholder for the missing values. All occurrences of missingValue will be imputed.
    * Note that null values are always treated as missing.
    * Default: Double.NaN
    *
    * @group param
    */
  final val missingValue: DoubleParam = new DoubleParam(this, "missingValue",
    "The placeholder for the missing values. All occurrences of missingValue will be imputed")

  /** @group getParam */
  def getMissingValue: Double = $(missingValue)

  /** Validates and transforms the input schema. */
  protected def validateAndTransformSchema(schema: StructType): StructType = {
    require($(inputCols).length == $(inputCols).distinct.length, s"inputCols contains" +
      s" duplicates: (${$(inputCols).mkString(", ")})")
    require($(outputCols).length == $(outputCols).distinct.length, s"outputCols contains" +
      s" duplicates: (${$(outputCols).mkString(", ")})")
    require($(inputCols).length == $(outputCols).length, s"inputCols(${$(inputCols).length})" +
      s" and outputCols(${$(outputCols).length}) should have the same length")
    val outputFields = $(inputCols).zip($(outputCols)).map { case (inputCol, outputCol) =>
      val inputField = schema(inputCol)
      SchemaUtils.checkColumnTypes(schema, inputCol, Seq(DoubleType, FloatType))
      StructField(outputCol, inputField.dataType, inputField.nullable)
    }
    StructType(schema ++ outputFields)
  }
}

/**
  * :: Experimental ::
  * Imputation estimator for completing missing values, either using the mean or the median
  * of the columns in which the missing values are located. The input columns should be of
  * DoubleType or FloatType. Currently Imputer does not support categorical features
  * (SPARK-15041) and possibly creates incorrect values for a categorical feature.
  *
  * Note that the mean/median value is computed after filtering out missing values.
  * All Null values in the input columns are treated as missing, and so are also imputed. For
  * computing median, DataFrameStatFunctions.approxQuantile is used with a relative error of 0.001.
  */
class Imputer(override val uid: String)
  extends Estimator[ImputerModel] with ImputerParams with DefaultParamsWritable {


  def this() = this(Identifiable.randomUID("imputer"))

  /** @group setParam */

  def setInputCols(value: Array[String]): this.type = set(inputCols, value)

  /** @group setParam */

  def setOutputCols(value: Array[String]): this.type = set(outputCols, value)

  /**
    * Imputation strategy. Available options are ["mean", "median"].
    *
    * @group setParam
    */

  def setStrategy(value: String): this.type = set(strategy, value)

  /** @group setParam */

  def setMissingValue(value: Double): this.type = set(missingValue, value)

  setDefault(strategy -> Imputer.mean, missingValue -> Double.NaN)

  override def fit(dataset: Dataset[_]): ImputerModel = {
    transformSchema(dataset.schema, logging = true)
    val spark = dataset.sparkSession
    import spark.implicits._
    val surrogates = $(inputCols).map { inputCol =>
      val ic = col(inputCol)
      val filtered = dataset.select(ic.cast(DoubleType))
        .filter(ic.isNotNull && ic =!= $(missingValue) && !ic.isNaN)
      if (filtered.take(1).length == 0) {
        throw new SparkException(s"surrogate cannot be computed. " +
          s"All the values in $inputCol are Null, Nan or missingValue(${$(missingValue)})")
      }
      val surrogate = $(strategy) match {
        case Imputer.mean => filtered.select(avg(inputCol)).as[Double].first()
        case Imputer.median => filtered.stat.approxQuantile(inputCol, Array(0.5), 0.001).head
      }
      surrogate
    }

    val rows = spark.sparkContext.parallelize(Seq(Row.fromSeq(surrogates)))
    val schema = StructType($(inputCols).map(col => StructField(col, DoubleType, nullable = false)))
    val surrogateDF = spark.createDataFrame(rows, schema)
    copyValues(new ImputerModel(uid, surrogateDF).setParent(this))
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  override def copy(extra: ParamMap): Imputer = defaultCopy(extra)
}


object Imputer extends DefaultParamsReadable[Imputer] {

  /** strategy names that Imputer currently supports. */
  private[sona] val mean = "mean"
  private[sona] val median = "median"


  override def load(path: String): Imputer = super.load(path)
}

/**
  * :: Experimental ::
  * Model fitted by [[Imputer]].
  *
  * @param surrogateDF a DataFrame containing inputCols and their corresponding surrogates,
  *                    which are used to replace the missing values in the input DataFrame.
  */


class ImputerModel private[angel](
                                   override val uid: String,
                                   val surrogateDF: DataFrame)
  extends Model[ImputerModel] with ImputerParams with MLWritable {

  import ImputerModel._

  /** @group setParam */
  def setInputCols(value: Array[String]): this.type = set(inputCols, value)

  /** @group setParam */
  def setOutputCols(value: Array[String]): this.type = set(outputCols, value)

  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema, logging = true)
    val surrogates = surrogateDF.select($(inputCols).map(col): _*).head().toSeq

    val newCols = $(inputCols).zip($(outputCols)).zip(surrogates).map {
      case ((inputCol, outputCol), surrogate) =>
        val inputType = dataset.schema(inputCol).dataType
        val ic = col(inputCol)
        when(ic.isNull, surrogate)
          .when(ic === $(missingValue), surrogate)
          .otherwise(ic)
          .cast(inputType)
    }
    var finalDataset = dataset
    (0 until newCols.length).foreach { index =>
      finalDataset = finalDataset.withColumn($(outputCols)(index), newCols(index))
    }
    finalDataset.toDF()
    //    dataset.withColumns($(outputCols), newCols).toDF()
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  override def copy(extra: ParamMap): ImputerModel = {
    val copied = new ImputerModel(uid, surrogateDF)
    copyValues(copied, extra).setParent(parent)
  }


  override def write: MLWriter = new ImputerModelWriter(this)
}


object ImputerModel extends MLReadable[ImputerModel] {

  private[ImputerModel] class ImputerModelWriter(instance: ImputerModel) extends MLWriter {

    override protected def saveImpl(path: String): Unit = {
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val dataPath = new Path(path, "data").toString
      instance.surrogateDF.repartition(1).write.parquet(dataPath)
    }
  }

  private class ImputerReader extends MLReader[ImputerModel] {

    private val className = classOf[ImputerModel].getName

    override def load(path: String): ImputerModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val dataPath = new Path(path, "data").toString
      val surrogateDF = sqlContext.read.parquet(dataPath)
      val model = new ImputerModel(metadata.uid, surrogateDF)
      metadata.getAndSetParams(model)
      model
    }
  }


  override def read: MLReader[ImputerModel] = new ImputerReader


  override def load(path: String): ImputerModel = super.load(path)
}
