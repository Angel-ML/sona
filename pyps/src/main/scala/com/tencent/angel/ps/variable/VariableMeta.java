package com.tencent.angel.ps.variable;

import com.tencent.angel.common.Meta;
import com.tencent.angel.matrix.MatrixContext;
import com.tencent.angel.model.MatrixLoadContext;
import com.tencent.angel.model.MatrixSaveContext;


import java.util.ArrayList;

public class VariableMeta extends Meta {
    private int numSlot;

    public VariableMeta(String name, String dtype, int dim, long[] shape, long validIndexNum, int numSlot) {
        super(name, dtype, dim, shape, validIndexNum);
        assert numSlot >= 1;
        this.numSlot = numSlot;

        assert dim == shape.length;
        if (rowType.isDense()) {
            int numRows = numSlot + 1;
            long numCols = 1L;
            for (int i = 0; i < dim; i++) {
                numCols *= shape[i];
            }

            matrixContext = new MatrixContext(name, numRows, numCols, validIndexNum, -1, -1, rowType);
        } else if (rowType.isSparse()) {
            assert dim <= 2 && dim > 0;
            if (dim == 1) {
                int numRows = numSlot + 1;
                long numCols = shape[0];
                matrixContext = new MatrixContext(name, numRows, numCols, validIndexNum, -1, -1, rowType);
            } else {
                int numRows = (numSlot + 1) * (int) shape[0];
                long numCols = shape[1];
                matrixContext = new MatrixContext(name, numRows, numCols, validIndexNum, -1, -1, rowType);
            }
        }

    }

    public VariableMeta(String name, String dtype, int dim, long[] shape, int numSlot) {
        this(name, dtype, dim, shape, -1, numSlot);
    }

    public VariableMeta(String name, String dtype, int dim, long[] shape) {
        this(name, dtype, dim, shape, -1, 1);
    }

    public VariableMeta(String name, String dtype, int numRows, long numCols) {
        this(name, dtype, numRows, numCols, -1);
    }

    public VariableMeta(String name, String dtype, int numRows, long numCols, long validIndexNum) {
        this(name, dtype, 2, new long[]{numRows, numCols}, validIndexNum, 1);
    }

    public MatrixContext getMatrixContext() {
        return matrixContext;
    }

    public MatrixSaveContext getMatrixSaveContext(String path, String formatClassName) {
        ArrayList<Integer> list = new ArrayList<Integer>();
        int originRows = matrixContext.getRowNum() / (numSlot + 1);
        for (int i = 0; i < originRows; i++) {
            list.add(i);
        }

        return new MatrixSaveContext(name, list, formatClassName);
    }

    public MatrixLoadContext getMatrixLoadContext(String path) {
        return new MatrixLoadContext(name, path);
    }

    public int getNumSlot() {
        return numSlot;
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj)) {
            return this.numSlot == ((VariableMeta) obj).numSlot;
        } else {
            return false;
        }
    }
}
