package com.tencent.angel.sona.ml.source.libffm

import org.apache.spark.sql.util.CaseInsensitiveMap

/**
  * Options for the LibFFM data source.
  */
private[libffm] class LibFFMOptions(@transient private val parameters: CaseInsensitiveMap[String])
  extends Serializable {

  import LibFFMOptions._

  def this(parameters: Map[String, String]) = this(CaseInsensitiveMap(parameters))

  /**
    * Number of features. If unspecified or nonpositive, the number of features will be determined
    * automatically at the cost of one additional pass.
    */
  val numFeatures: Option[Long] = parameters.get(NUM_FEATURES).map(_.toLong).filter(_ > 0)

  val numFields: Option[Int] = parameters.get(NUM_FIELDS).map(_.toInt).filter(_ > 0)

  val isSparse: Boolean = parameters.getOrElse(VECTOR_TYPE, SPARSE_VECTOR_TYPE) match {
    case SPARSE_VECTOR_TYPE => true
    case DENSE_VECTOR_TYPE => false
    case o => throw new IllegalArgumentException(s"Invalid value `$o` for parameter " +
      s"`$VECTOR_TYPE`. Expected types are `sparse` and `dense`.")
  }

  val isLongKey: Boolean = parameters.getOrElse(KEY_TYPE, INT_KEY_TYPE) match {
    case LONG_KEY_TYPE => true
    case INT_KEY_TYPE => false
    case o => throw new IllegalArgumentException(s"Invalid value `$o` for parameter " +
      s"`$KEY_TYPE`. Expected types are `int` and `long`.")
  }
}

private[libffm] object LibFFMOptions {
  val NUM_FEATURES = "numFeatures"
  val NUM_FIELDS = "numFields"
  val VECTOR_TYPE = "vectorType"
  val DENSE_VECTOR_TYPE = "dense"
  val SPARSE_VECTOR_TYPE = "sparse"
  val KEY_TYPE = "keyType"
  val INT_KEY_TYPE = "int"
  val LONG_KEY_TYPE = "long"
}