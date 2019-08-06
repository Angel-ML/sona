package org.apache.spark.angel.ml.util

import org.apache.spark.angel.ml.linalg.Vector

object DataUtils {
  case class Example(label: Double, weight: Double, features: Vector)
}
