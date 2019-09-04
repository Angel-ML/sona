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

import edu.emory.mathcs.jtransforms.dct._
import org.apache.spark.linalg.{VectorUDT, Vectors}
import com.tencent.angel.sona.ml.UnaryTransformer
import com.tencent.angel.sona.ml.param.BooleanParam
import com.tencent.angel.sona.ml.util.{DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.types.DataType
import com.tencent.angel.sona.ml.util.DefaultParamsReadable
import org.apache.spark.linalg

/**
  * A feature transformer that takes the 1D discrete cosine transform of a real vector. No zero
  * padding is performed on the input vector.
  * It returns a real vector of the same length representing the DCT. The return vector is scaled
  * such that the transform matrix is unitary (aka scaled DCT-II).
  *
  * More information on <a href="https://en.wikipedia.org/wiki/Discrete_cosine_transform#DCT-II">
  * DCT-II in Discrete cosine transform (Wikipedia)</a>.
  */
class DCT(override val uid: String)
  extends UnaryTransformer[linalg.Vector, linalg.Vector, DCT] with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("dct"))

  /**
    * Indicates whether to perform the inverse DCT (true) or forward DCT (false).
    * Default: false
    *
    * @group param
    */

  def inverse: BooleanParam = new BooleanParam(
    this, "inverse", "Set transformer to perform inverse DCT")

  /** @group setParam */

  def setInverse(value: Boolean): this.type = set(inverse, value)

  /** @group getParam */

  def getInverse: Boolean = $(inverse)

  setDefault(inverse -> false)

  override protected def createTransformFunc: linalg.Vector => linalg.Vector = { vec =>
    val result = vec.toArray
    val jTransformer = new DoubleDCT_1D(result.length)
    if ($(inverse)) jTransformer.inverse(result, true) else jTransformer.forward(result, true)
    Vectors.dense(result)
  }

  override protected def validateInputType(inputType: DataType): Unit = {
    require(inputType.isInstanceOf[VectorUDT],
      s"Input type must be ${(new VectorUDT).catalogString} but got ${inputType.catalogString}.")
  }

  override protected def outputDataType: DataType = new VectorUDT
}


object DCT extends DefaultParamsReadable[DCT] {


  override def load(path: String): DCT = super.load(path)
}
