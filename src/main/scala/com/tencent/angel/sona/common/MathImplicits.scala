package com.tencent.angel.sona.common

import com.tencent.angel.ml.core.conf.SharedConf
import com.tencent.angel.ml.math2.VFactory
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector}


object MathImplicits {

  type AVector = com.tencent.angel.ml.math2.vector.Vector

  implicit def toAngelVector(bv: Vector)(implicit dim: Long): AVector = {
    (SharedConf.valueType(), bv, SharedConf.keyType()) match {
      case ("double", DenseVector(dv: Array[Double]), "int") =>
        VFactory.denseDoubleVector(dv)
      case ("float", DenseVector(dv: Array[Double]), "int") =>
        VFactory.denseFloatVector(dv.map(_.toFloat))
      case ("double", SparseVector(_: Int, idx: Array[Int], values: Array[Double]), "int") =>
        VFactory.sparseDoubleVector(dim.toInt, idx, values)
      case ("float", SparseVector(_: Int, idx: Array[Int], values: Array[Double]), "int") =>
        VFactory.sparseFloatVector(dim.toInt, idx, values.map(_.toFloat))
      case ("double", SparseVector(_: Int, idx: Array[Int], values: Array[Double]), "long") =>
        VFactory.sparseLongKeyDoubleVector(dim.toInt, idx.map(_.toLong), values)
      case ("float", SparseVector(_: Int, idx: Array[Int], values: Array[Double]), "long") =>
        VFactory.sparseLongKeyFloatVector(dim.toInt, idx.map(_.toLong), values.map(_.toFloat))
    }
  }
}
