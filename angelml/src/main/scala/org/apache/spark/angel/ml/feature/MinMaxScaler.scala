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

package org.apache.spark.angel.ml.feature

import org.apache.hadoop.fs.Path
import org.apache.spark.angel.ml.{Estimator, Model, linalg}
import org.apache.spark.angel.ml.linalg.{VectorUDT, Vectors}
import org.apache.spark.angel.ml.param.{DoubleParam, ParamMap, Params}
import org.apache.spark.angel.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.angel.ml.stat.Statistics
import org.apache.spark.angel.ml.util.{DefaultParamsReadable, DefaultParamsReader, DefaultParamsWritable, DefaultParamsWriter, Identifiable, MLReadable, MLReader, MLUtils, MLWritable, MLWriter, SchemaUtils}
import org.apache.spark.annotation.Since
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructField, StructType}

/**
  * Params for [[MinMaxScaler]] and [[MinMaxScalerModel]].
  */
private[feature] trait MinMaxScalerParams extends Params with HasInputCol with HasOutputCol {

  /**
    * lower bound after transformation, shared by all features
    * Default: 0.0
    *
    * @group param
    */
  val min: DoubleParam = new DoubleParam(this, "min",
    "lower bound of the output feature range")

  /** @group getParam */
  def getMin: Double = $(min)

  /**
    * upper bound after transformation, shared by all features
    * Default: 1.0
    *
    * @group param
    */
  val max: DoubleParam = new DoubleParam(this, "max",
    "upper bound of the output feature range")

  /** @group getParam */
  def getMax: Double = $(max)

  /** Validates and transforms the input schema. */
  protected def validateAndTransformSchema(schema: StructType): StructType = {
    require($(min) < $(max), s"The specified min(${$(min)}) is larger or equal to max(${$(max)})")
    SchemaUtils.checkColumnType(schema, $(inputCol), new VectorUDT)
    require(!schema.fieldNames.contains($(outputCol)),
      s"Output column ${$(outputCol)} already exists.")
    val outputFields = schema.fields :+ StructField($(outputCol), new VectorUDT, false)
    StructType(outputFields)
  }

}

/**
  * Rescale each feature individually to a common range [min, max] linearly using column summary
  * statistics, which is also known as min-max normalization or Rescaling. The rescaled value for
  * feature E is calculated as:
  *
  * <blockquote>
  * $$
  * Rescaled(e_i) = \frac{e_i - E_{min}}{E_{max} - E_{min}} * (max - min) + min
  * $$
  * </blockquote>
  *
  * For the case \(E_{max} == E_{min}\), \(Rescaled(e_i) = 0.5 * (max + min)\).
  *
  * @note Since zero values will probably be transformed to non-zero values, output of the
  *       transformer will be DenseVector even for sparse input.
  */
@Since("1.5.0")
class MinMaxScaler @Since("1.5.0")(@Since("1.5.0") override val uid: String)
  extends Estimator[MinMaxScalerModel] with MinMaxScalerParams with DefaultParamsWritable {

  @Since("1.5.0")
  def this() = this(Identifiable.randomUID("minMaxScal"))

  setDefault(min -> 0.0, max -> 1.0)

  /** @group setParam */
  @Since("1.5.0")
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  @Since("1.5.0")
  def setOutputCol(value: String): this.type = set(outputCol, value)

  /** @group setParam */
  @Since("1.5.0")
  def setMin(value: Double): this.type = set(min, value)

  /** @group setParam */
  @Since("1.5.0")
  def setMax(value: Double): this.type = set(max, value)

  @Since("2.0.0")
  override def fit(dataset: Dataset[_]): MinMaxScalerModel = {
    transformSchema(dataset.schema, logging = true)
    val input: RDD[linalg.Vector] = dataset.select($(inputCol)).rdd.map {
      case Row(v: linalg.Vector) => v
    }
    val summary = Statistics.colStats(input)
    copyValues(new MinMaxScalerModel(uid, summary.min, summary.max).setParent(this))
  }

  @Since("1.5.0")
  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  @Since("1.5.0")
  override def copy(extra: ParamMap): MinMaxScaler = defaultCopy(extra)
}

@Since("1.6.0")
object MinMaxScaler extends DefaultParamsReadable[MinMaxScaler] {

  @Since("1.6.0")
  override def load(path: String): MinMaxScaler = super.load(path)
}

/**
  * Model fitted by [[MinMaxScaler]].
  *
  * @param originalMin min value for each original column during fitting
  * @param originalMax max value for each original column during fitting
  *
  *                    TODO: The transformer does not yet set the metadata in the output column (SPARK-8529).
  */
@Since("1.5.0")
class MinMaxScalerModel private[angel](
                                          @Since("1.5.0") override val uid: String,
                                          @Since("2.0.0") val originalMin: linalg.Vector,
                                          @Since("2.0.0") val originalMax: linalg.Vector)
  extends Model[MinMaxScalerModel] with MinMaxScalerParams with MLWritable {

  import MinMaxScalerModel._

  /** @group setParam */
  @Since("1.5.0")
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  @Since("1.5.0")
  def setOutputCol(value: String): this.type = set(outputCol, value)

  /** @group setParam */
  @Since("1.5.0")
  def setMin(value: Double): this.type = set(min, value)

  /** @group setParam */
  @Since("1.5.0")
  def setMax(value: Double): this.type = set(max, value)

  @Since("2.0.0")
  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema, logging = true)
    val originalRange = (originalMax.asBreeze - originalMin.asBreeze).toArray
    val minArray = originalMin.toArray

    val reScale = udf { (vector: linalg.Vector) =>
      val scale = $(max) - $(min)

      // 0 in sparse vector will probably be rescaled to non-zero
      val values = vector.toArray
      val size = values.length
      var i = 0
      while (i < size) {
        if (!values(i).isNaN) {
          val raw = if (originalRange(i) != 0) (values(i) - minArray(i)) / originalRange(i) else 0.5
          values(i) = raw * scale + $(min)
        }
        i += 1
      }
      Vectors.dense(values)
    }

    dataset.withColumn($(outputCol), reScale(col($(inputCol))))
  }

  @Since("1.5.0")
  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  @Since("1.5.0")
  override def copy(extra: ParamMap): MinMaxScalerModel = {
    val copied = new MinMaxScalerModel(uid, originalMin, originalMax)
    copyValues(copied, extra).setParent(parent)
  }

  @Since("1.6.0")
  override def write: MLWriter = new MinMaxScalerModelWriter(this)
}

@Since("1.6.0")
object MinMaxScalerModel extends MLReadable[MinMaxScalerModel] {

  private[MinMaxScalerModel]
  class MinMaxScalerModelWriter(instance: MinMaxScalerModel) extends MLWriter {

    private case class Data(originalMin: linalg.Vector, originalMax: linalg.Vector)

    override protected def saveImpl(path: String): Unit = {
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val data = new Data(instance.originalMin, instance.originalMax)
      val dataPath = new Path(path, "data").toString
      sparkSession.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
    }
  }

  private class MinMaxScalerModelReader extends MLReader[MinMaxScalerModel] {

    private val className = classOf[MinMaxScalerModel].getName

    override def load(path: String): MinMaxScalerModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val dataPath = new Path(path, "data").toString
      val data = sparkSession.read.parquet(dataPath)
      val Row(originalMin: linalg.Vector, originalMax: linalg.Vector) =
        MLUtils.convertVectorColumnsToML(data, "originalMin", "originalMax")
          .select("originalMin", "originalMax")
          .head()
      val model = new MinMaxScalerModel(metadata.uid, originalMin, originalMax)
      metadata.getAndSetParams(model)
      model
    }
  }

  @Since("1.6.0")
  override def read: MLReader[MinMaxScalerModel] = new MinMaxScalerModelReader

  @Since("1.6.0")
  override def load(path: String): MinMaxScalerModel = super.load(path)
}
