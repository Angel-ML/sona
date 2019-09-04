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

import breeze.linalg.{DenseVector => BDV}
import org.apache.hadoop.fs.Path
import com.tencent.angel.sona.ml.{Estimator, Model}
import org.apache.spark.linalg.{DenseVector, IntSparseVector, LongSparseVector, VectorUDT, Vectors}
import com.tencent.angel.sona.ml.param.{IntParam, ParamMap, ParamValidators, Params}
import com.tencent.angel.sona.ml.param.shared.{HasInputCol, HasOutputCol}
import com.tencent.angel.sona.ml.util._
import org.apache.spark.sql.util.SchemaUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.linalg

/**
  * Params for [[IDF]] and [[IDFModel]].
  */
private[sona] trait IDFBase extends Params with HasInputCol with HasOutputCol {

  /**
    * The minimum number of documents in which a term should appear.
    * Default: 0
    *
    * @group param
    */
  final val minDocFreq = new IntParam(
    this, "minDocFreq", "minimum number of documents in which a term should appear for filtering" +
      " (>= 0)", ParamValidators.gtEq(0))

  setDefault(minDocFreq -> 0)

  /** @group getParam */
  def getMinDocFreq: Int = $(minDocFreq)

  /**
    * Validate and transform the input schema.
    */
  protected def validateAndTransformSchema(schema: StructType): StructType = {
    SchemaUtils.checkColumnType(schema, $(inputCol), new VectorUDT)
    SchemaUtils.appendColumn(schema, $(outputCol), new VectorUDT)
  }
}

/**
  * Compute the Inverse Document Frequency (IDF) given a collection of documents.
  */
final class IDF(override val uid: String)
  extends Estimator[IDFModel] with IDFBase with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("idf"))

  /** @group setParam */

  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */

  def setOutputCol(value: String): this.type = set(outputCol, value)

  /** @group setParam */

  def setMinDocFreq(value: Int): this.type = set(minDocFreq, value)


  override def fit(dataset: Dataset[_]): IDFModel = {
    transformSchema(dataset.schema, logging = true)
    val input: RDD[linalg.Vector] = dataset.select($(inputCol)).rdd.map {
      case Row(v: linalg.Vector) => v
    }

    val initValue = new IDF.DocumentFrequencyAggregator(minDocFreq = $(minDocFreq))
    val idf = input.treeAggregate(initValue)(
      seqOp = (df, v) => df.add(v),
      combOp = (df1, df2) => df1.merge(df2)
    ).idf()

    copyValues(new IDFModel(uid, idf).setParent(this))
  }


  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }


  override def copy(extra: ParamMap): IDF = defaultCopy(extra)
}


object IDF extends DefaultParamsReadable[IDF] {


  override def load(path: String): IDF = super.load(path)

  /** Document frequency aggregator. */
  class DocumentFrequencyAggregator(val minDocFreq: Int) extends Serializable {

    /** number of documents */
    private var m = 0L
    /** document frequency vector */
    private var df: BDV[Long] = _


    def this() = this(0)

    /** Adds a new document. */
    def add(doc: linalg.Vector): this.type = {
      if (isEmpty) {
        df = BDV.zeros(doc.size.toInt)
      }
      doc match {
        case IntSparseVector(_, indices, values) =>
          val nnz = indices.length
          var k = 0
          while (k < nnz) {
            if (values(k) > 0) {
              df(indices(k)) += 1L
            }
            k += 1
          }
        case LongSparseVector(_, indices, values) =>
          val nnz = indices.length
          var k = 0
          if (indices.last > Int.MaxValue.toLong) {
            throw new IndexOutOfBoundsException(s"the Indices of ${doc.getClass} is out of Int range.")
          }
          while (k < nnz) {
            if (values(k) > 0) {
              df(indices(k).toInt) += 1L
            }
            k += 1
          }
        case DenseVector(values) =>
          val n = values.length
          var j = 0
          while (j < n) {
            if (values(j) > 0.0) {
              df(j) += 1L
            }
            j += 1
          }
        case other =>
          throw new UnsupportedOperationException(
            s"Only sparse and dense vectors are supported but got ${other.getClass}.")
      }
      m += 1L
      this
    }

    /** Merges another. */
    def merge(other: DocumentFrequencyAggregator): this.type = {
      if (!other.isEmpty) {
        m += other.m
        if (df == null) {
          df = other.df.copy
        } else {
          df += other.df
        }
      }
      this
    }

    private def isEmpty: Boolean = m == 0L

    /** Returns the current IDF vector. */
    def idf(): linalg.Vector = {
      if (isEmpty) {
        throw new IllegalStateException("Haven't seen any document yet.")
      }
      val n = df.length
      val inv = new Array[Double](n)
      var j = 0
      while (j < n) {
        /*
         * If the term is not present in the minimum
         * number of documents, set IDF to 0. This
         * will cause multiplication in IDFModel to
         * set TF-IDF to 0.
         *
         * Since arrays are initialized to 0 by default,
         * we just omit changing those entries.
         */
        if (df(j) >= minDocFreq) {
          inv(j) = math.log((m + 1.0) / (df(j) + 1.0))
        }
        j += 1
      }
      Vectors.dense(inv)
    }
  }

}

/**
  * Model fitted by [[IDF]].
  */
class IDFModel private[angel](
                               override val uid: String,
                               val idf: linalg.Vector)
  extends Model[IDFModel] with IDFBase with MLWritable {

  import IDFModel._

  /** @group setParam */

  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */

  def setOutputCol(value: String): this.type = set(outputCol, value)


  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema, logging = true)
    // TODO: Make the idfModel.transform natively in ml framework to avoid extra conversion.
    val bcIdf = dataset.sparkSession.sparkContext.broadcast(idf)
    val idfUDF = udf { vec: linalg.Vector => IDFModel.transform(bcIdf.value, vec) }
    dataset.withColumn($(outputCol), idfUDF(col($(inputCol))))
  }


  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }


  override def copy(extra: ParamMap): IDFModel = {
    val copied = new IDFModel(uid, idf)
    copyValues(copied, extra).setParent(parent)
  }


  override def write: MLWriter = new IDFModelWriter(this)
}


object IDFModel extends MLReadable[IDFModel] {

  private[IDFModel] class IDFModelWriter(instance: IDFModel) extends MLWriter {

    private case class Data(idf: linalg.Vector)

    override protected def saveImpl(path: String): Unit = {
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val data = Data(instance.idf)
      val dataPath = new Path(path, "data").toString
      sparkSession.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
    }
  }

  private class IDFModelReader extends MLReader[IDFModel] {

    private val className = classOf[IDFModel].getName

    override def load(path: String): IDFModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val dataPath = new Path(path, "data").toString
      val data = sparkSession.read.parquet(dataPath)
      val Row(idf: linalg.Vector) = MLUtils.convertVectorColumnsToML(data, "idf")
        .select("idf")
        .head()
      val model = new IDFModel(metadata.uid, idf)
      metadata.getAndSetParams(model)
      model
    }
  }


  override def read: MLReader[IDFModel] = new IDFModelReader


  override def load(path: String): IDFModel = super.load(path)

  /**
    * Transforms a term frequency (TF) vector to a TF-IDF vector with a IDF vector
    *
    * @param idf an IDF vector
    * @param v   a term frequency vector
    * @return a TF-IDF vector
    */
  def transform(idf: linalg.Vector, v: linalg.Vector): linalg.Vector = {
    val n = v.size.toInt
    v match {
      case IntSparseVector(size, indices, values) =>
        val nnz = indices.length
        val newValues = new Array[Double](nnz)
        var k = 0
        while (k < nnz) {
          newValues(k) = values(k) * idf(indices(k))
          k += 1
        }
        Vectors.sparse(n, indices, newValues)
      case LongSparseVector(size, indices, values) =>
        val nnz = indices.length
        val newValues = new Array[Double](nnz)
        var k = 0
        while (k < nnz) {
          newValues(k) = values(k) * idf(indices(k))
          k += 1
        }
        Vectors.sparse(n, indices, newValues)
      case DenseVector(values) =>
        val newValues = new Array[Double](n)
        var j = 0
        while (j < n) {
          newValues(j) = values(j) * idf(j)
          j += 1
        }
        Vectors.dense(newValues)
      case other =>
        throw new UnsupportedOperationException(
          s"Only sparse and dense vectors are supported but got ${other.getClass}.")
    }
  }
}
