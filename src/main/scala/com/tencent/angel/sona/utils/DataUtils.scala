package com.tencent.angel.sona.utils

import org.apache.spark.ml.linalg.Vector

object DataUtils {
  case class Example(label: Double, weight: Double, features: Vector)
}
