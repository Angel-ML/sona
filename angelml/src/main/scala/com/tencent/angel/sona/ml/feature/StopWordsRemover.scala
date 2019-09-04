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

import java.util.Locale

import com.tencent.angel.sona.ml.Transformer
import com.tencent.angel.sona.ml.param.{BooleanParam, Param, ParamMap, ParamValidators, StringArrayParam}
import com.tencent.angel.sona.ml.param.shared.{HasInputCol, HasOutputCol}
import com.tencent.angel.sona.ml.util._
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}
import org.apache.spark.sql.util.SchemaUtils
import org.apache.spark.util.DataTypeUtil

/**
  * A feature transformer that filters out stop words from input.
  *
  * @note null values from input array are preserved unless adding null to stopWords
  *       explicitly.
  * @see <a href="http://en.wikipedia.org/wiki/Stop_words">Stop words (Wikipedia)</a>
  */
class StopWordsRemover(override val uid: String)
  extends Transformer with HasInputCol with HasOutputCol with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("stopWords"))

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  /**
    * The words to be filtered out.
    * Default: English stop words
    *
    * @see `StopWordsRemover.loadDefaultStopWords()`
    * @group param
    */
  val stopWords: StringArrayParam =
    new StringArrayParam(this, "stopWords", "the words to be filtered out")

  /** @group setParam */
  def setStopWords(value: Array[String]): this.type = set(stopWords, value)

  /** @group getParam */
  def getStopWords: Array[String] = $(stopWords)

  /**
    * Whether to do a case sensitive comparison over the stop words.
    * Default: false
    *
    * @group param
    */
  val caseSensitive: BooleanParam = new BooleanParam(this, "caseSensitive",
    "whether to do a case-sensitive comparison over the stop words")

  /** @group setParam */
  def setCaseSensitive(value: Boolean): this.type = set(caseSensitive, value)

  /** @group getParam */
  def getCaseSensitive: Boolean = $(caseSensitive)

  /**
    * Locale of the input for case insensitive matching. Ignored when [[caseSensitive]]
    * is true.
    * Default: Locale.getDefault.toString
    *
    * @group param
    */
  val locale: Param[String] = new Param[String](this, "locale",
    "Locale of the input for case insensitive matching. Ignored when caseSensitive is true.",
    ParamValidators.inArray[String](Locale.getAvailableLocales.map(_.toString)))

  /** @group setParam */
  def setLocale(value: String): this.type = set(locale, value)

  /** @group getParam */
  def getLocale: String = $(locale)

  setDefault(stopWords -> StopWordsRemover.loadDefaultStopWords("english"),
    caseSensitive -> false, locale -> Locale.getDefault.toString)

  override def transform(dataset: Dataset[_]): DataFrame = {
    val outputSchema = transformSchema(dataset.schema)
    val t = if ($(caseSensitive)) {
      val stopWordsSet = $(stopWords).toSet
      udf { terms: Seq[String] =>
        terms.filter(s => !stopWordsSet.contains(s))
      }
    } else {
      val lc = new Locale($(locale))
      val toLower = (s: String) => if (s != null) s.toLowerCase(lc) else s
      val lowerStopWords = $(stopWords).map(toLower(_)).toSet
      udf { terms: Seq[String] =>
        terms.filter(s => !lowerStopWords.contains(toLower(s)))
      }
    }
    val metadata = outputSchema($(outputCol)).metadata
    dataset.select(col("*"), t(col($(inputCol))).as($(outputCol), metadata))
  }

  override def transformSchema(schema: StructType): StructType = {
    val inputType = schema($(inputCol)).dataType
    require(DataTypeUtil.sameType(inputType, ArrayType(StringType)), "Input type must be " +
      s"${ArrayType(StringType).catalogString} but got ${inputType.catalogString}.")
    SchemaUtils.appendColumn(schema, $(outputCol), inputType, schema($(inputCol)).nullable)
  }

  override def copy(extra: ParamMap): StopWordsRemover = defaultCopy(extra)
}


object StopWordsRemover extends DefaultParamsReadable[StopWordsRemover] {
  private[sona]
  val supportedLanguages = Set("danish", "dutch", "english", "finnish", "french", "german",
    "hungarian", "italian", "norwegian", "portuguese", "russian", "spanish", "swedish", "turkish")

  override def load(path: String): StopWordsRemover = super.load(path)

  /**
    * Loads the default stop words for the given language.
    * Supported languages: danish, dutch, english, finnish, french, german, hungarian,
    * italian, norwegian, portuguese, russian, spanish, swedish, turkish
    *
    * @see <a href="http://anoncvs.postgresql.org/cvsweb.cgi/pgsql/src/backend/snowball/stopwords/">
    *      here</a>
    */
  def loadDefaultStopWords(language: String): Array[String] = {
    require(supportedLanguages.contains(language),
      s"$language is not in the supported language list: ${supportedLanguages.mkString(", ")}.")
    val is = getClass.getResourceAsStream(s"/com/tencent/angel/sona/ml/feature/stopwords/$language.txt")
    scala.io.Source.fromInputStream(is)(scala.io.Codec.UTF8).getLines().toArray
  }
}
