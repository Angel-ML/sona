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
import org.apache.spark.linalg.{VectorUDT, Vectors}
import com.tencent.angel.sona.ml.param.{ParamMap, Params}
import com.tencent.angel.sona.ml.param.shared.{HasInputCol, HasOutputCol}
import com.tencent.angel.sona.ml.stat.Statistics
import com.tencent.angel.sona.ml.util._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.linalg
import org.apache.spark.sql.util.SONASchemaUtils

/**
  * Params for [[MaxAbsScaler]] and [[MaxAbsScalerModel]].
  */
private[sona] trait MaxAbsScalerParams extends Params with HasInputCol with HasOutputCol {

  /** Validates and transforms the input schema. */
  protected def validateAndTransformSchema(schema: StructType): StructType = {
    SONASchemaUtils.checkColumnType(schema, $(inputCol), new VectorUDT)
    require(!schema.fieldNames.contains($(outputCol)),
      s"Output column ${$(outputCol)} already exists.")
    val outputFields = schema.fields :+ StructField($(outputCol), new VectorUDT, false)
    StructType(outputFields)
  }
}

/**
  * Rescale each feature individually to range [-1, 1] by dividing through the largest maximum
  * absolute value in each feature. It does not shift/center the data, and thus does not destroy
  * any sparsity.
  */
class MaxAbsScaler(override val uid: String)
  extends Estimator[MaxAbsScalerModel] with MaxAbsScalerParams with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("maxAbsScal"))

  /** @group setParam */

  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */

  def setOutputCol(value: String): this.type = set(outputCol, value)


  override def fit(dataset: Dataset[_]): MaxAbsScalerModel = {
    transformSchema(dataset.schema, logging = true)
    val input: RDD[linalg.Vector] = dataset.select($(inputCol)).rdd.map {
      case Row(v: linalg.Vector) => v
    }
    val summary = Statistics.colStats(input)
    val minVals = summary.min.toArray
    val maxVals = summary.max.toArray
    val n = minVals.length
    val maxAbs = Array.tabulate(n) { i => math.max(math.abs(minVals(i)), math.abs(maxVals(i))) }

    copyValues(new MaxAbsScalerModel(uid, Vectors.dense(maxAbs)).setParent(this))
  }


  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }


  override def copy(extra: ParamMap): MaxAbsScaler = defaultCopy(extra)
}


object MaxAbsScaler extends DefaultParamsReadable[MaxAbsScaler] {
  override def load(path: String): MaxAbsScaler = super.load(path)
}

/**
  * Model fitted by [[MaxAbsScaler]].
  *
  */
class MaxAbsScalerModel private[angel](
                                        override val uid: String,
                                        val maxAbs: linalg.Vector)
  extends Model[MaxAbsScalerModel] with MaxAbsScalerParams with MLWritable {

  import MaxAbsScalerModel._

  /** @group setParam */

  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */

  def setOutputCol(value: String): this.type = set(outputCol, value)


  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema, logging = true)
    // TODO: this looks hack, we may have to handle sparse and dense vectors separately.
    val maxAbsUnzero = Vectors.dense(maxAbs.toArray.map(x => if (x == 0) 1 else x))
    val reScale = udf { (vector: linalg.Vector) =>
      val brz = vector.asBreeze / maxAbsUnzero.asBreeze
      Vectors.fromBreeze(brz)
    }
    dataset.withColumn($(outputCol), reScale(col($(inputCol))))
  }


  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }


  override def copy(extra: ParamMap): MaxAbsScalerModel = {
    val copied = new MaxAbsScalerModel(uid, maxAbs)
    copyValues(copied, extra).setParent(parent)
  }


  override def write: MLWriter = new MaxAbsScalerModelWriter(this)
}


object MaxAbsScalerModel extends MLReadable[MaxAbsScalerModel] {

  private[MaxAbsScalerModel]
  class MaxAbsScalerModelWriter(instance: MaxAbsScalerModel) extends MLWriter {

    private case class Data(maxAbs: linalg.Vector)

    override protected def saveImpl(path: String): Unit = {
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val data = new Data(instance.maxAbs)
      val dataPath = new Path(path, "data").toString
      sparkSession.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
    }
  }

  private class MaxAbsScalerModelReader extends MLReader[MaxAbsScalerModel] {

    private val className = classOf[MaxAbsScalerModel].getName

    override def load(path: String): MaxAbsScalerModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val dataPath = new Path(path, "data").toString
      val Row(maxAbs: linalg.Vector) = sparkSession.read.parquet(dataPath)
        .select("maxAbs")
        .head()
      val model = new MaxAbsScalerModel(metadata.uid, maxAbs)
      metadata.getAndSetParams(model)
      model
    }
  }


  override def read: MLReader[MaxAbsScalerModel] = new MaxAbsScalerModelReader


  override def load(path: String): MaxAbsScalerModel = super.load(path)
}
