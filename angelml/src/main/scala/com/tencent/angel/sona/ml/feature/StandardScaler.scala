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
import com.tencent.angel.sona.ml.{Estimator, Model}
import com.tencent.angel.sona.ml.param.{BooleanParam, ParamMap, Params}
import com.tencent.angel.sona.ml.param.shared.{HasInputCol, HasOutputCol}
import com.tencent.angel.sona.ml.stat.MultivariateOnlineSummarizer
import com.tencent.angel.sona.ml.util._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.linalg
import org.apache.spark.linalg._
import org.apache.spark.sql.util.SONASchemaUtils

/**
  * Params for [[StandardScaler]] and [[StandardScalerModel]].
  */
private[sona] trait StandardScalerParams extends Params with HasInputCol with HasOutputCol {
  /**
    * Whether to center the data with mean before scaling.
    * It will build a dense output, so take care when applying to sparse input.
    * Default: false
    *
    * @group param
    */
  val withMean: BooleanParam = new BooleanParam(this, "withMean",
    "Whether to center data with mean")

  /** @group getParam */
  def getWithMean: Boolean = $(withMean)

  /**
    * Whether to scale the data to unit standard deviation.
    * Default: true
    *
    * @group param
    */
  val withStd: BooleanParam = new BooleanParam(this, "withStd",
    "Whether to scale the data to unit standard deviation")

  /** @group getParam */
  def getWithStd: Boolean = $(withStd)

  /** Validates and transforms the input schema. */
  protected def validateAndTransformSchema(schema: StructType): StructType = {
    SONASchemaUtils.checkColumnType(schema, $(inputCol), new VectorUDT)
    require(!schema.fieldNames.contains($(outputCol)),
      s"Output column ${$(outputCol)} already exists.")
    val outputFields = schema.fields :+ StructField($(outputCol), new VectorUDT, false)
    StructType(outputFields)
  }

  setDefault(withMean -> false, withStd -> true)
}

/**
  * Standardizes features by removing the mean and scaling to unit variance using column summary
  * statistics on the samples in the training set.
  *
  * The "unit std" is computed using the
  * <a href="https://en.wikipedia.org/wiki/Standard_deviation#Corrected_sample_standard_deviation">
  * corrected sample standard deviation</a>,
  * which is computed as the square root of the unbiased sample variance.
  */
class StandardScaler(override val uid: String)
  extends Estimator[StandardScalerModel] with StandardScalerParams with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("stdScal"))

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  /** @group setParam */
  def setWithMean(value: Boolean): this.type = set(withMean, value)

  /** @group setParam */
  def setWithStd(value: Boolean): this.type = set(withStd, value)

  override def fit(dataset: Dataset[_]): StandardScalerModel = {
    transformSchema(dataset.schema, logging = true)
    val input: RDD[linalg.Vector] = dataset.select($(inputCol)).rdd.map {
      case Row(v: linalg.Vector) => v
    }

    val summary = input.treeAggregate(new MultivariateOnlineSummarizer)(
      (aggregator, data) => aggregator.add(data),
      (aggregator1, aggregator2) => aggregator1.merge(aggregator2))

    val std = summary.variance match {
      case DenseVector(values) =>
        Vectors.dense(values.map(v => Math.sqrt(v)))
      case IntSparseVector(size, index, values) =>
        Vectors.sparse(size, index, values.map(v => Math.sqrt(v)))
      case _ => throw new Exception("Vector type is not support!")
    }

    copyValues(new StandardScalerModel(uid, std, summary.mean).setParent(this))
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  override def copy(extra: ParamMap): StandardScaler = defaultCopy(extra)
}


object StandardScaler extends DefaultParamsReadable[StandardScaler] {
  override def load(path: String): StandardScaler = super.load(path)
}

/**
  * Model fitted by [[StandardScaler]].
  *
  * @param std  Standard deviation of the StandardScalerModel
  * @param mean Mean of the StandardScalerModel
  */
class StandardScalerModel private[angel](
                                          override val uid: String,
                                          val std: linalg.Vector,
                                          val mean: linalg.Vector)
  extends Model[StandardScalerModel] with StandardScalerParams with MLWritable {

  import StandardScalerModel._

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  private lazy val shift: Array[Double] = mean.toArray

  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema, logging = true)

    // TODO: Make the transformer natively in ml framework to avoid extra conversion.
    val transformer: linalg.Vector => linalg.Vector = vector => {
      require(mean.size == vector.size)
      if ($(withMean)) {
        // By default, Scala generates Java methods for member variables. So every time when
        // the member variables are accessed, `invokespecial` will be called which is expensive.
        // This can be avoid by having a local reference of `shift`.
        val localShift = shift
        // Must have a copy of the values since it will be modified in place
        val values = vector match {
          // specially handle DenseVector because its toArray does not clone already
          case d: DenseVector => d.values.clone()
          case v: linalg.Vector => v.toArray
        }
        val size = values.length
        if ($(withStd)) {
          var i = 0
          while (i < size) {
            values(i) = if (std(i) != 0.0) (values(i) - localShift(i)) * (1.0 / std(i)) else 0.0
            i += 1
          }
        } else {
          var i = 0
          while (i < size) {
            values(i) -= localShift(i)
            i += 1
          }
        }
        Vectors.dense(values)
      } else if ($(withStd)) {
        vector match {
          case DenseVector(vs) =>
            val values = vs.clone()
            val size = values.length
            var i = 0
            while (i < size) {
              values(i) *= (if (std(i) != 0.0) 1.0 / std(i) else 0.0)
              i += 1
            }
            Vectors.dense(values)
          case IntSparseVector(size, indices, vs) =>
            // For sparse vector, the `index` array inside sparse vector object will not be changed,
            // so we can re-use it to save memory.
            val values = vs.clone()
            val nnz = values.length
            var i = 0
            while (i < nnz) {
              values(i) *= (if (std(indices(i)) != 0.0) 1.0 / std(indices(i)) else 0.0)
              i += 1
            }
            Vectors.sparse(size, indices, values)
          case v => throw new IllegalArgumentException("Do not support vector type " + v.getClass)
        }
      } else {
        // Note that it's safe since we always assume that the data in RDD should be immutable.
        vector
      }
    }

    val scale = udf(transformer)
    dataset.withColumn($(outputCol), scale(col($(inputCol))))
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  override def copy(extra: ParamMap): StandardScalerModel = {
    val copied = new StandardScalerModel(uid, std, mean)
    copyValues(copied, extra).setParent(parent)
  }

  override def write: MLWriter = new StandardScalerModelWriter(this)
}


object StandardScalerModel extends MLReadable[StandardScalerModel] {

  private[StandardScalerModel]
  class StandardScalerModelWriter(instance: StandardScalerModel) extends MLWriter {

    private case class Data(std: linalg.Vector, mean: linalg.Vector)

    override protected def saveImpl(path: String): Unit = {
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val data = Data(instance.std, instance.mean)
      val dataPath = new Path(path, "data").toString
      sparkSession.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
    }
  }

  private class StandardScalerModelReader extends MLReader[StandardScalerModel] {

    private val className = classOf[StandardScalerModel].getName

    override def load(path: String): StandardScalerModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val dataPath = new Path(path, "data").toString
      val data = sparkSession.read.parquet(dataPath)
      val Row(std: linalg.Vector, mean: linalg.Vector) = MLUtils.convertVectorColumnsToML(data, "std", "mean")
        .select("std", "mean")
        .head()
      val model = new StandardScalerModel(metadata.uid, std, mean)
      metadata.getAndSetParams(model)
      model
    }
  }

  override def read: MLReader[StandardScalerModel] = new StandardScalerModelReader

  override def load(path: String): StandardScalerModel = super.load(path)
}
