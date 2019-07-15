package com.tencent.angel.common;

import com.tencent.angel.matrix.MatrixContext;
import com.tencent.angel.ml.servingmath2.utils.RowType;
import com.tencent.angel.model.MatrixLoadContext;
import com.tencent.angel.model.MatrixSaveContext;
import com.tencent.angel.ps.tensor.TensorMeta;


import java.util.Arrays;


public abstract class Meta {
    protected String name;
    protected String dtype;
    protected int dim;
    protected long[] shape;
    protected long validIndexNum;
    protected RowType rowType;
    protected MatrixContext matrixContext;

    public Meta(String name, String dtype, int dim, long[] shape, long validIndexNum) {
        this.name = name;
        this.dtype = dtype;
        this.dim = dim;
        this.shape = shape;
        this.validIndexNum = validIndexNum;
        this.rowType = Utils.getRowType(dtype);
    }

    public String getName() {
        return name;
    }

    public String getDtype() {
        return dtype;
    }

    public int getDim() {
        return dim;
    }

    public long[] getShape() {
        return shape;
    }

    public long getValidIndexNum() {
        return validIndexNum;
    }

    public RowType getRowType() {
        return rowType;
    }

    public abstract MatrixContext getMatrixContext();

    public abstract MatrixSaveContext getMatrixSaveContext(String path, String formatClassName);

    public abstract MatrixLoadContext getMatrixLoadContext(String path);

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        } else if (obj instanceof TensorMeta) {
            TensorMeta other = (TensorMeta) obj;

            if (!this.name.equalsIgnoreCase(other.name)) {
                return false;
            }

            if (this.dim != other.dim) {
                return false;
            }

            if (!Arrays.equals(this.shape, other.shape)) {
                return false;
            }

            if (!this.dtype.equalsIgnoreCase(other.dtype)) {
                return false;
            }

            return this.validIndexNum == other.validIndexNum;
        } else {
            return false;
        }
    }

}
