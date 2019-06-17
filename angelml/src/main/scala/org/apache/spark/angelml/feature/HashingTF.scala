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

package org.apache.spark.angelml.feature

import org.apache.spark.SparkException
import org.apache.spark.annotation.Since
import org.apache.spark.angelml.Transformer
import org.apache.spark.angelml.attribute.AttributeGroup
import org.apache.spark.angelml.param._
import org.apache.spark.angelml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.angelml.util._
import org.apache.spark.angelml.linalg.{Vector, Vectors}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{ArrayType, StructType}
import org.apache.spark.unsafe.hash.Murmur3_x86_32._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.Utils

import scala.collection.mutable

/**
 * Maps a sequence of terms to their term frequencies using the hashing trick.
 * Currently we use Austin Appleby's MurmurHash 3 algorithm (MurmurHash3_x86_32)
 * to calculate the hash code value for the term object.
 * Since a simple modulo is used to transform the hash function to a column index,
 * it is advisable to use a power of two as the numFeatures parameter;
 * otherwise the features will not be mapped evenly to the columns.
 */
@Since("1.2.0")
class HashingTF @Since("1.4.0") (@Since("1.4.0") override val uid: String)
  extends Transformer with HasInputCol with HasOutputCol with DefaultParamsWritable {

  import HashingTF._

  private var hashAlgorithm = HashingTF.Murmur3

  @Since("1.2.0")
  def this() = this(Identifiable.randomUID("hashingTF"))

  /** @group setParam */
  @Since("1.4.0")
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  @Since("1.4.0")
  def setOutputCol(value: String): this.type = set(outputCol, value)

  /**
    * Number of features. Should be greater than 0.
    * (default = 2^18^)
    *
    * @group param
    */
  @Since("1.2.0")
  val numFeatures = new IntParam(this, "numFeatures", "number of features (> 0)",
    ParamValidators.gt(0))

  /**
    * Binary toggle to control term frequency counts.
    * If true, all non-zero counts are set to 1.  This is useful for discrete probabilistic
    * models that model binary events rather than integer counts.
    * (default = false)
    *
    * @group param
    */
  @Since("2.0.0")
  val binary = new BooleanParam(this, "binary", "If true, all non zero counts are set to 1. " +
    "This is useful for discrete probabilistic models that model binary events rather " +
    "than integer counts")

  setDefault(numFeatures -> (1 << 18), binary -> false)

  /** @group getParam */
  @Since("1.2.0")
  def getNumFeatures: Int = $(numFeatures)

  /** @group setParam */
  @Since("1.2.0")
  def setNumFeatures(value: Int): this.type = set(numFeatures, value)

  /** @group getParam */
  @Since("2.0.0")
  def getBinary: Boolean = $(binary)

  /** @group setParam */
  @Since("2.0.0")
  def setBinary(value: Boolean): this.type = set(binary, value)

  /**
    * Set the hash algorithm used when mapping term to integer.
    * (default: murmur3)
    */
  @Since("2.0.0")
  def setHashAlgorithm(value: String): this.type = {
    hashAlgorithm = value
    this
  }

  private def getHashFunction: Any => Int = hashAlgorithm match {
    case Murmur3 => murmur3Hash
    case Native => nativeHash
    case _ =>
      // This should never happen.
      throw new IllegalArgumentException(
        s"HashingTF does not recognize hash algorithm $hashAlgorithm")
  }

  @Since("2.0.0")
  override def transform(dataset: Dataset[_]): DataFrame = {
    val outputSchema = transformSchema(dataset.schema)
    // TODO: Make the hashingTF.transform natively in ml framework to avoid extra conversion.

    val transUDF = (document: Iterable[_]) => {
      val termFrequencies = mutable.HashMap.empty[Int, Double]
      val setTF = if ($(binary)) {
        i: Int => 1.0
      } else {
        i: Int => termFrequencies.getOrElse(i, 0.0) + 1.0
      }
      val hashFunc: Any => Int = getHashFunction
      document.foreach { term =>
        val i = Utils.nonNegativeMod(hashFunc(term), $(numFeatures))
        termFrequencies.put(i, setTF(i))
      }

      Vectors.sparse($(numFeatures), termFrequencies.toSeq)
    }

    val t = udf { terms: Seq[_] => transUDF(terms) }
    val metadata = outputSchema($(outputCol)).metadata
    dataset.select(col("*"), t(col($(inputCol))).as($(outputCol), metadata))
  }

  @Since("1.4.0")
  override def transformSchema(schema: StructType): StructType = {
    val inputType = schema($(inputCol)).dataType
    require(inputType.isInstanceOf[ArrayType],
      s"The input column must be ${ArrayType.simpleString}, but got ${inputType.catalogString}.")
    val attrGroup = new AttributeGroup($(outputCol), $(numFeatures))
    SchemaUtils.appendColumn(schema, attrGroup.toStructField())
  }

  @Since("1.4.1")
  override def copy(extra: ParamMap): HashingTF = defaultCopy(extra)
}

@Since("1.6.0")
object HashingTF extends DefaultParamsReadable[HashingTF] {

  @Since("1.6.0")
  override def load(path: String): HashingTF = super.load(path)

  private[HashingTF] val Native: String = "native"

  private[HashingTF] val Murmur3: String = "murmur3"

  private[spark] val seed = 42

  private[HashingTF] def nativeHash(term: Any): Int = term.##

  /**
    * Calculate a hash code value for the term object using
    * Austin Appleby's MurmurHash 3 algorithm (MurmurHash3_x86_32).
    * This is the default hash algorithm used from Spark 2.0 onwards.
    */
  private[spark] def murmur3Hash(term: Any): Int = {
    term match {
      case null => seed
      case b: Boolean => hashInt(if (b) 1 else 0, seed)
      case b: Byte => hashInt(b, seed)
      case s: Short => hashInt(s, seed)
      case i: Int => hashInt(i, seed)
      case l: Long => hashLong(l, seed)
      case f: Float => hashInt(java.lang.Float.floatToIntBits(f), seed)
      case d: Double => hashLong(java.lang.Double.doubleToLongBits(d), seed)
      case s: String =>
        val utf8 = UTF8String.fromString(s)
        hashUnsafeBytes(utf8.getBaseObject, utf8.getBaseOffset, utf8.numBytes(), seed)
      case _ => throw new SparkException("HashingTF with murmur3 algorithm does not " +
        s"support type ${term.getClass.getCanonicalName} of input data.")
    }
  }
}
