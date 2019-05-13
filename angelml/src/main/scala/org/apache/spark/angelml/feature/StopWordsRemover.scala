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

import java.util.Locale

import org.apache.spark.annotation.Since
import org.apache.spark.angelml.Transformer
import org.apache.spark.angelml.param._
import org.apache.spark.angelml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.angelml.util._
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}

/**
 * A feature transformer that filters out stop words from input.
 *
 * @note null values from input array are preserved unless adding null to stopWords
 * explicitly.
 *
 * @see <a href="http://en.wikipedia.org/wiki/Stop_words">Stop words (Wikipedia)</a>
 */
@Since("1.5.0")
class StopWordsRemover @Since("1.5.0") (@Since("1.5.0") override val uid: String)
  extends Transformer with HasInputCol with HasOutputCol with DefaultParamsWritable {

  @Since("1.5.0")
  def this() = this(Identifiable.randomUID("stopWords"))

  /** @group setParam */
  @Since("1.5.0")
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  @Since("1.5.0")
  def setOutputCol(value: String): this.type = set(outputCol, value)

  /**
   * The words to be filtered out.
   * Default: English stop words
   * @see `StopWordsRemover.loadDefaultStopWords()`
   * @group param
   */
  @Since("1.5.0")
  val stopWords: StringArrayParam =
    new StringArrayParam(this, "stopWords", "the words to be filtered out")

  /** @group setParam */
  @Since("1.5.0")
  def setStopWords(value: Array[String]): this.type = set(stopWords, value)

  /** @group getParam */
  @Since("1.5.0")
  def getStopWords: Array[String] = $(stopWords)

  /**
   * Whether to do a case sensitive comparison over the stop words.
   * Default: false
   * @group param
   */
  @Since("1.5.0")
  val caseSensitive: BooleanParam = new BooleanParam(this, "caseSensitive",
    "whether to do a case-sensitive comparison over the stop words")

  /** @group setParam */
  @Since("1.5.0")
  def setCaseSensitive(value: Boolean): this.type = set(caseSensitive, value)

  /** @group getParam */
  @Since("1.5.0")
  def getCaseSensitive: Boolean = $(caseSensitive)

  /**
   * Locale of the input for case insensitive matching. Ignored when [[caseSensitive]]
   * is true.
   * Default: Locale.getDefault.toString
   * @group param
   */
  @Since("2.4.0")
  val locale: Param[String] = new Param[String](this, "locale",
    "Locale of the input for case insensitive matching. Ignored when caseSensitive is true.",
    ParamValidators.inArray[String](Locale.getAvailableLocales.map(_.toString)))

  /** @group setParam */
  @Since("2.4.0")
  def setLocale(value: String): this.type = set(locale, value)

  /** @group getParam */
  @Since("2.4.0")
  def getLocale: String = $(locale)

  setDefault(stopWords -> StopWordsRemover.loadDefaultStopWords("english"),
    caseSensitive -> false, locale -> Locale.getDefault.toString)

  @Since("2.0.0")
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

  @Since("1.5.0")
  override def transformSchema(schema: StructType): StructType = {
    val inputType = schema($(inputCol)).dataType
    require(inputType.sameType(ArrayType(StringType)), "Input type must be " +
      s"${ArrayType(StringType).catalogString} but got ${inputType.catalogString}.")
    SchemaUtils.appendColumn(schema, $(outputCol), inputType, schema($(inputCol)).nullable)
  }

  @Since("1.5.0")
  override def copy(extra: ParamMap): StopWordsRemover = defaultCopy(extra)
}

@Since("1.6.0")
object StopWordsRemover extends DefaultParamsReadable[StopWordsRemover] {

  private[feature]
  val supportedLanguages = Set("danish", "dutch", "english", "finnish", "french", "german",
    "hungarian", "italian", "norwegian", "portuguese", "russian", "spanish", "swedish", "turkish")

  @Since("1.6.0")
  override def load(path: String): StopWordsRemover = super.load(path)

  /**
   * Loads the default stop words for the given language.
   * Supported languages: danish, dutch, english, finnish, french, german, hungarian,
   * italian, norwegian, portuguese, russian, spanish, swedish, turkish
   * @see <a href="http://anoncvs.postgresql.org/cvsweb.cgi/pgsql/src/backend/snowball/stopwords/">
   * here</a>
   */
  @Since("2.0.0")
  def loadDefaultStopWords(language: String): Array[String] = {
    require(supportedLanguages.contains(language),
      s"$language is not in the supported language list: ${supportedLanguages.mkString(", ")}.")
    val is = getClass.getResourceAsStream(s"/org/apache/spark/angelml/feature/stopwords/$language.txt")
    scala.io.Source.fromInputStream(is)(scala.io.Codec.UTF8).getLines().toArray
  }
}
