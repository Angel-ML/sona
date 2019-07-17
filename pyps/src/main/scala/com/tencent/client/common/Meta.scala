package com.tencent.client.common

import java.util

import com.tencent.angel.matrix.MatrixContext
import com.tencent.angel.ml.servingmath2.utils.RowType
import com.tencent.angel.model.MatrixLoadContext
import com.tencent.angel.model.MatrixSaveContext
import com.tencent.client.ps.tensor.TensorMeta


abstract class Meta(val name: String, val dtype: String, val dim: Int, val shape:Array[Long], val validIndexNum: Long) {
    val rowType: RowType = Utils.getRowType(dtype)
    protected val matrixContext: MatrixContext

    def getMatrixContext: MatrixContext

    def getMatrixSaveContext(path: String, formatClassName: String): MatrixSaveContext

    def getMatrixLoadContext(path: String): MatrixLoadContext

    override def equals(obj: Any): Boolean = {
        if (this == obj) {
            true
        } else if (obj.isInstanceOf[TensorMeta]) {
            val other = obj.asInstanceOf[TensorMeta]

            if (!this.name.equalsIgnoreCase(other.name)) {
                return false
            }

            if (this.dim != other.dim) {
                return false
            }

            if (!util.Arrays.equals(this.shape, other.shape)) {
                return false
            }

            if (!this.dtype.equalsIgnoreCase(other.dtype)) {
                return false
            }

            this.validIndexNum == other.validIndexNum
        } else {
            false
        }
    }

}
