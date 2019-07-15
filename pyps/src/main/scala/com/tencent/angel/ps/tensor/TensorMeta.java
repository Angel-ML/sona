package com.tencent.angel.ps.tensor;

import com.tencent.angel.common.Meta;
import com.tencent.angel.matrix.MatrixContext;
import com.tencent.angel.model.MatrixLoadContext;
import com.tencent.angel.model.MatrixSaveContext;


public class TensorMeta extends Meta {
    public TensorMeta(String name, String dtype, int dim, long[] shape, long validIndexNum) {
        super(name, dtype, dim, shape, validIndexNum);

        assert dim == shape.length;
        if (rowType.isDense()) {
            long numCols = 1L;
            for (int i = 0; i < dim; i++) {
                numCols *= shape[i];
            }

            matrixContext = new MatrixContext(name, 1, numCols, validIndexNum, -1, -1, rowType);
        } else if (rowType.isSparse()) {
            assert dim <= 2 && dim > 0;
            if (dim == 1) {
                long numCols = shape[0];
                matrixContext = new MatrixContext(name, 1, numCols, validIndexNum, -1, -1, rowType);
            } else { // dim == 2
                matrixContext = new MatrixContext(name, (int) shape[0], shape[1], validIndexNum, -1, -1, rowType);
            }
        }

    }

    public TensorMeta(String name, String dtype, int dim, long[] shape) {
        this(name, dtype, dim, shape, -1);
    }

    public TensorMeta(String name, String dtype, int numRows, long numCols) {
        this(name, dtype, numRows, numRows, -1);

    }

    public TensorMeta(String name, String dtype, int numRows, long numCols, long validIndexNum) {
        super(name, dtype, 2, new long[]{numRows, numCols}, validIndexNum);
    }

    public MatrixContext getMatrixContext() {
        return matrixContext;
    }

    public MatrixSaveContext getMatrixSaveContext(String path, String formatClassName) {
        return new MatrixSaveContext(name, formatClassName);
    }

    public MatrixLoadContext getMatrixLoadContext(String path) {
        return new MatrixLoadContext(name, path);
    }


}
