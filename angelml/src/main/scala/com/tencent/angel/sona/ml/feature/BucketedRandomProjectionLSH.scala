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

import scala.util.Random
import breeze.linalg.normalize
import org.apache.hadoop.fs.Path
import com.tencent.angel.sona.ml.param._
import com.tencent.angel.sona.ml.param.shared.HasSeed
import com.tencent.angel.sona.ml.util._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.linalg
import org.apache.spark.linalg.{BLAS, Matrices, Matrix, VectorUDT, Vectors}
import org.apache.spark.sql.util.SONASchemaUtils

/**
  * :: Experimental ::
  *
  * Params for [[BucketedRandomProjectionLSH]].
  */
private[sona] trait BucketedRandomProjectionLSHParams extends Params {

  /**
    * The length of each hash bucket, a larger bucket lowers the false negative rate. The number of
    * buckets will be `(max L2 norm of input vectors) / bucketLength`.
    *
    *
    * If input vectors are normalized, 1-10 times of pow(numRecords, -1/inputDim) would be a
    * reasonable value
    *
    * @group param
    */
  val bucketLength: DoubleParam = new DoubleParam(this, "bucketLength",
    "the length of each hash bucket, a larger bucket lowers the false negative rate.",
    ParamValidators.gt(0))

  /** @group getParam */
  final def getBucketLength: Double = $(bucketLength)
}

/**
  * :: Experimental ::
  *
  * Model produced by [[BucketedRandomProjectionLSH]], where multiple random vectors are stored. The
  * vectors are normalized to be unit vectors and each vector is used in a hash function:
  * `h_i(x) = floor(r_i.dot(x) / bucketLength)`
  * where `r_i` is the i-th random unit vector. The number of buckets will be `(max L2 norm of input
  * vectors) / bucketLength`.
  *
  * @param randUnitVectors An array of random unit vectors. Each vector represents a hash function.
  */
class BucketedRandomProjectionLSHModel private[sona](
                                                      override val uid: String,
                                                      private[sona] val randUnitVectors: Array[linalg.Vector])
  extends LSHModel[BucketedRandomProjectionLSHModel] with BucketedRandomProjectionLSHParams {

  /** @group setParam */

  override def setInputCol(value: String): this.type = super.set(inputCol, value)

  /** @group setParam */

  override def setOutputCol(value: String): this.type = super.set(outputCol, value)


  override def hashFunction(elems: linalg.Vector): Array[linalg.Vector] = {
    val hashValues = randUnitVectors.map(
      randUnitVector => Math.floor(BLAS.dot(elems, randUnitVector) / $(bucketLength))
    )
    // TODO: Output vectors of dimension numHashFunctions in SPARK-18450
    hashValues.map(Vectors.dense(_))
  }


  override def keyDistance(x: linalg.Vector, y: linalg.Vector): Double = {
    Math.sqrt(Vectors.sqdist(x, y))
  }


  override def hashDistance(x: Seq[linalg.Vector], y: Seq[linalg.Vector]): Double = {
    // Since it's generated by hashing, it will be a pair of dense vectors.
    x.zip(y).map(vectorPair => Vectors.sqdist(vectorPair._1, vectorPair._2)).min
  }


  override def copy(extra: ParamMap): BucketedRandomProjectionLSHModel = {
    val copied = new BucketedRandomProjectionLSHModel(uid, randUnitVectors).setParent(parent)
    copyValues(copied, extra)
  }


  override def write: MLWriter = {
    new BucketedRandomProjectionLSHModel.BucketedRandomProjectionLSHModelWriter(this)
  }
}

/**
  * :: Experimental ::
  *
  * This [[BucketedRandomProjectionLSH]] implements Locality Sensitive Hashing functions for
  * Euclidean distance metrics.
  *
  * The input is dense or sparse vectors, each of which represents a point in the Euclidean
  * distance space. The output will be vectors of configurable dimension. Hash values in the
  * same dimension are calculated by the same hash function.
  *
  * References:
  *
  * 1. <a href="https://en.wikipedia.org/wiki/Locality-sensitive_hashing#Stable_distributions">
  * Wikipedia on Stable Distributions</a>
  *
  * 2. Wang, Jingdong et al. "Hashing for similarity search: A survey." arXiv preprint
  * arXiv:1408.2927 (2014).
  */
class BucketedRandomProjectionLSH(override val uid: String)
  extends LSH[BucketedRandomProjectionLSHModel]
    with BucketedRandomProjectionLSHParams with HasSeed {


  override def setInputCol(value: String): this.type = super.setInputCol(value)


  override def setOutputCol(value: String): this.type = super.setOutputCol(value)


  override def setNumHashTables(value: Int): this.type = super.setNumHashTables(value)


  def this() = {
    this(Identifiable.randomUID("brp-lsh"))
  }

  /** @group setParam */

  def setBucketLength(value: Double): this.type = set(bucketLength, value)

  /** @group setParam */

  def setSeed(value: Long): this.type = set(seed, value)


  override protected[this] def createRawLSHModel(
                                                  inputDim: Int): BucketedRandomProjectionLSHModel = {
    val rand = new Random($(seed))
    val randUnitVectors: Array[linalg.Vector] = {
      Array.fill($(numHashTables)) {
        val randArray = Array.fill(inputDim)(rand.nextGaussian())
        Vectors.fromBreeze(normalize(breeze.linalg.Vector(randArray)))
      }
    }
    new BucketedRandomProjectionLSHModel(uid, randUnitVectors)
  }


  override def transformSchema(schema: StructType): StructType = {
    SONASchemaUtils.checkColumnType(schema, $(inputCol), new VectorUDT)
    validateAndTransformSchema(schema)
  }


  override def copy(extra: ParamMap): this.type = defaultCopy(extra)
}


object BucketedRandomProjectionLSH extends DefaultParamsReadable[BucketedRandomProjectionLSH] {
  override def load(path: String): BucketedRandomProjectionLSH = super.load(path)
}


object BucketedRandomProjectionLSHModel extends MLReadable[BucketedRandomProjectionLSHModel] {
  override def read: MLReader[BucketedRandomProjectionLSHModel] = {
    new BucketedRandomProjectionLSHModelReader
  }


  override def load(path: String): BucketedRandomProjectionLSHModel = super.load(path)

  private[BucketedRandomProjectionLSHModel] class BucketedRandomProjectionLSHModelWriter(
                                                                                          instance: BucketedRandomProjectionLSHModel) extends MLWriter {

    // TODO: Save using the existing format of Array[Vector] once SPARK-12878 is resolved.
    private case class Data(randUnitVectors: Matrix)

    override protected def saveImpl(path: String): Unit = {
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val numRows = instance.randUnitVectors.length
      require(numRows > 0)
      val numCols = instance.randUnitVectors.head.size.toInt
      val values = instance.randUnitVectors.map(_.toArray).reduce(Array.concat(_, _))
      val randMatrix = Matrices.dense(numRows, numCols, values)
      val data = Data(randMatrix)
      val dataPath = new Path(path, "data").toString
      sparkSession.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
    }
  }

  private class BucketedRandomProjectionLSHModelReader
    extends MLReader[BucketedRandomProjectionLSHModel] {

    /** Checked against metadata when loading model */
    private val className = classOf[BucketedRandomProjectionLSHModel].getName

    override def load(path: String): BucketedRandomProjectionLSHModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)

      val dataPath = new Path(path, "data").toString
      val data = sparkSession.read.parquet(dataPath)
      val Row(randUnitVectors: Matrix) = MLUtils.convertMatrixColumnsToML(data, "randUnitVectors")
        .select("randUnitVectors")
        .head()
      val model = new BucketedRandomProjectionLSHModel(metadata.uid,
        randUnitVectors.rowIter.toArray)

      metadata.getAndSetParams(model)
      model
    }
  }

}
