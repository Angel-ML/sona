package com.tencent.angel.common

import com.tencent.angel.ml.servingmath2.matrix._
import com.tencent.angel.ml.servingmath2.utils.RowType
import com.tencent.angel.ml.servingmath2.vector._


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

  def toBytes(vec: Vector): Array[Byte] = {
    /*
    vec match {
      case v: IntDoubleVector if v.isDense =>
      case v: IntDoubleVector if v.isSparse =>
      case v: IntDoubleVector if v.isSorted =>
      case v: IntFloatVector if v.isDense =>
      case v: IntFloatVector if v.isSparse =>
      case v: IntFloatVector if v.isSorted =>
      case v: IntLongVector if v.isDense =>
      case v: IntLongVector if v.isSparse =>
      case v: IntLongVector if v.isSorted =>
      case v: IntIntVector if v.isDense =>
      case v: IntIntVector if v.isSparse =>
      case v: IntIntVector if v.isSorted =>
      case v: LongDoubleVector if v.isSparse =>
      case v: LongDoubleVector if v.isSorted =>
      case v: LongFloatVector if v.isSparse =>
      case v: LongFloatVector if v.isSorted =>
      case v: LongLongVector if v.isSparse =>
      case v: LongLongVector if v.isSorted =>
      case v: LongIntVector if v.isSparse =>
      case v: LongIntVector if v.isSorted =>
    }

     */

    null
  }

  def fromBytes(bytes: Array[Byte], meta: Meta): Matrix = {
    null
  }
}
