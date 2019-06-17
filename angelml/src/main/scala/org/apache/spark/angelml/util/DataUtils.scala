package org.apache.spark.angelml.util

import org.apache.spark.angelml.linalg.Vector

object DataUtils {
  case class Example(label: Double, weight: Double, features: Vector)
}
