package com.tencent.client.common

import com.tencent.angel.ml.servingmath2.MFactory
import com.tencent.angel.ml.servingmath2.matrix._
import com.tencent.angel.ml.servingmath2.utils.RowType
import com.tencent.angel.ml.servingmath2.vector._
import java.util
import java.lang.{Long => JLong}

import org.apache.hadoop.conf.Configuration

import scala.collection.JavaConverters._
import scala.collection.mutable

object Utils {
  def getRowType(dtype: String): RowType = {
    if (dtype.equalsIgnoreCase("int") || dtype.equalsIgnoreCase("int32")) {
      RowType.T_INT_DENSE
    } else if (dtype.equalsIgnoreCase("long")) {
      RowType.T_LONG_DENSE
    } else if (dtype.equalsIgnoreCase("float")) {
      RowType.T_FLOAT_DENSE
    } else if (dtype.equalsIgnoreCase("double")) {
      RowType.T_DOUBLE_DENSE
    } else if (dtype.equalsIgnoreCase("sp_int") || dtype.equalsIgnoreCase("sp_int32")) {
      RowType.T_INT_SPARSE
    } else if (dtype.equalsIgnoreCase("sp_long")) {
      RowType.T_LONG_SPARSE
    } else if (dtype.equalsIgnoreCase("sp_float")) {
      RowType.T_FLOAT_SPARSE
    } else if (dtype.equalsIgnoreCase("sp_double")) {
      RowType.T_DOUBLE_SPARSE
    } else if (dtype.equalsIgnoreCase("lk_int") || dtype.equalsIgnoreCase("lk_int32")) {
      RowType.T_INT_SPARSE_LONGKEY
    } else if (dtype.equalsIgnoreCase("lk_long")) {
      RowType.T_LONG_SPARSE_LONGKEY
    } else if (dtype.equalsIgnoreCase("lk_float")) {
      RowType.T_FLOAT_SPARSE_LONGKEY
    } else if (dtype.equalsIgnoreCase("lk_double")) {
      RowType.T_DOUBLE_SPARSE_LONGKEY
    } else {
      throw new Exception("data type is not allowed for dense")
    }
  }

  def vectorArray2Matrix(vectors: Array[Vector]): Matrix = {
    val vec = vectors.head
    val mat = vec match {
      case _: IntDoubleVector => MFactory.rbIntDoubleMatrix(vectors.map(_.asInstanceOf[IntDoubleVector]))
      case _: IntFloatVector => MFactory.rbIntFloatMatrix(vectors.map(_.asInstanceOf[IntFloatVector]))
      case _: IntLongVector => MFactory.rbIntLongMatrix(vectors.map(_.asInstanceOf[IntLongVector]))
      case _: IntIntVector => MFactory.rbIntIntMatrix(vectors.map(_.asInstanceOf[IntIntVector]))
      case _: LongDoubleVector => MFactory.rbLongDoubleMatrix(vectors.map(_.asInstanceOf[LongDoubleVector]))
      case _: LongFloatVector => MFactory.rbLongFloatMatrix(vectors.map(_.asInstanceOf[LongFloatVector]))
      case _: LongLongVector => MFactory.rbLongLongMatrix(vectors.map(_.asInstanceOf[LongLongVector]))
      case _: LongIntVector => MFactory.rbLongIntMatrix(vectors.map(_.asInstanceOf[LongIntVector]))
      case _ => throw new Exception("vector type is not supported!")
    }
    mat.setMatrixId(vec.getMatrixId)
    mat
  }

  def vector2Matrix(vec: Vector): Matrix = {
    val mat = vec match {
      case v: IntDoubleVector => MFactory.rbIntDoubleMatrix(Array(v))
      case v: IntFloatVector => MFactory.rbIntFloatMatrix(Array(v))
      case v: IntLongVector => MFactory.rbIntLongMatrix(Array(v))
      case v: IntIntVector => MFactory.rbIntIntMatrix(Array(v))
      case v: LongDoubleVector => MFactory.rbLongDoubleMatrix(Array(v))
      case v: LongFloatVector => MFactory.rbLongFloatMatrix(Array(v))
      case v: LongLongVector => MFactory.rbLongLongMatrix(Array(v))
      case v: LongIntVector => MFactory.rbLongIntMatrix(Array(v))
      case _ => throw new Exception("vector type is not supported!")
    }

    mat.setMatrixId(vec.getMatrixId)

    mat
  }

  def toArray(arr: util.List[JLong]): Array[Long] = {
    (0 until arr.size()).toArray.map{i =>
      val ele: Long = arr.get(i)

      ele
    }
  }

  def toMap(map: util.Map[String, String]): Map[String, String] = {
    val iter = map.entrySet().iterator()
    val list = new mutable.ListBuffer[(String, String)]()
    while(iter.hasNext) {
      val entry = iter.next()
      list.append(entry.getKey -> entry.getValue)
    }

    list.toMap
  }

  def fillConf(params:  util.Map[String, String], conf: Configuration): Unit = {
    val iter = params.entrySet().iterator()
    while(iter.hasNext) {
      val entry = iter.next()
      conf.set(entry.getKey, entry.getValue)
    }
  }
}
