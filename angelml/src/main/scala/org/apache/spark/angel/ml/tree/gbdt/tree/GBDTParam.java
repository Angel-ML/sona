package org.apache.spark.angel.ml.tree.gbdt.tree;

import org.apache.spark.angel.ml.tree.regression.RegTParam;
import org.apache.spark.angel.ml.tree.util.MathUtil;

import java.io.Serializable;
import java.util.Arrays;

public class GBDTParam implements Serializable {
    public RegTParam regTParam;  // param of regression tree
    public boolean isRegression = DEFAULT_IS_REGRESSION;  // whether it is a regression task
    public int numClass = DEFAULT_NUM_CLASS;  // number of classes/labels
    public int numRound = DEFAULT_NUM_ROUND;  // number of rounds

    public float initLearningRate = DEFAULT_INIT_LEARNING_RATE;
    public float minChildWeight = DEFAULT_MIN_CHILD_WEIGHT;  // minimum amount of hessian (weight) allowed for a child
    public float regAlpha = DEFAULT_REG_ALPHA;  // L1 regularization factor
    public float regLambda = DEFAULT_REG_LAMBDA;  // L2 regularization factor
    public float maxLeafWeight = DEFAULT_MAX_LEAF_WEIGHT; // maximum leaf weight, default 0 means no constraints
    public boolean fullHessian = DEFAULT_FULL_HESSIAN;  // whether to use full hessian matrix instead of diagonal
    public boolean multiTree = DEFAULT_MULTI_TREE;  // whether to use multiple one-vs-rest trees in multi-classification

    public String lossFunc; // name of loss function
    public String[] evalMetrics; // name of eval metric

    public GBDTParam(RegTParam regTParam) {
        setRegTParam(regTParam);
    }

    public GBDTParam setRegTParam(RegTParam regTParam) {
        this.regTParam = regTParam;
        return this;
    }

    public GBDTParam setRegression(boolean regression) {
        isRegression = regression;
        return this;
    }

    public GBDTParam setNumClass(int numClass) {
        this.numClass = numClass;
        return this;
    }

    public GBDTParam setNumRound(int numRound) {
        this.numRound = numRound;
        return this;
    }

    public GBDTParam setInitLearningRate(float initLearningRate) {
        this.initLearningRate = initLearningRate;
        return this;
    }

    public GBDTParam setMinChildWeight(float minChildWeight) {
        this.minChildWeight = minChildWeight;
        return this;
    }

    public GBDTParam setRegAlpha(float regAlpha) {
        this.regAlpha = regAlpha;
        return this;
    }

    public GBDTParam setRegLambda(float regLambda) {
        this.regLambda = regLambda;
        return this;
    }

    public GBDTParam setMaxLeafWeight(float maxLeafWeight) {
        this.maxLeafWeight = maxLeafWeight;
        return this;
    }

    public GBDTParam setFullHessian(boolean fullHessian) {
        this.fullHessian = fullHessian;
        return this;
    }

    public GBDTParam setMultiTree(boolean multiTree) {
        this.multiTree = multiTree;
        return this;
    }

    public GBDTParam setLossFunc(String lossFunc) {
        this.lossFunc = lossFunc;
        return this;
    }

    public GBDTParam setEvalMetrics(String[] evalMetrics) {
        this.evalMetrics = evalMetrics;
        return this;
    }

    /**
     * Whether each tree is a multi-label classifier.
     * For regression task, binary-classification task,
     * and multi-classification with one-vs-rest trees,
     * each tree is a binary-label classifier.
     * Otherwise, each tree is a multi-label classifier.
     *
     * @return true if tree leaf outputs a vector as weights/predictions, false otherwise
     */
    public boolean isLeafVector() {
        return !isRegression && numClass > 2 && !multiTree;
    }

    public boolean isMultiClassMultiTree() {
        return !isRegression && numClass > 2 && multiTree;
    }

    /**
     * Whether the sum of hessian satisfies weight for binary classification.
     *
     * @param sumHess sum of hessian values
     * @return true if satisfied, false otherwise
     */
    public boolean satisfyWeight(double sumHess) {
        return sumHess >= minChildWeight;
    }

    /**
     * Whether the sum of hessian satisfies weight for binary classification.
     *
     * @param sumGrad sum of gradient values
     * @param sumHess sum of hessian values
     * @return true if satisfied, false otherwise
     */
    public boolean satisfyWeight(double sumGrad, double sumHess) {
        return sumGrad != 0.0f && satisfyWeight(sumHess);
    }

    /**
     * Whether the sum of hessian satisfies weight for multi classification.
     * Since hessian matrix is positive, we have det(hess) <= a11*a22*...*akk,
     * thus we approximate det(hess) with a11*a22*...*akk.
     *
     * @param sumHess sum of hessian values
     * @return true if satisfied, false otherwise
     */
    public boolean satisfyWeight(double[] sumHess) {
        if (minChildWeight == 0.0f) return true;
        double w = 1.0;
        if (!fullHessian) {
            for (double h : sumHess) w *= h;
        } else {
            for (int k = 0; k < numClass; k++) {
                int index = MathUtil.indexOfLowerTriangularMatrix(k, k);
                w *= sumHess[index];
            }
        }
        return w >= minChildWeight;
    }

    /**
     * Whether the sum of hessian satisfies weight for multi classification.
     * Since hessian matrix is positive, we have det(hess) <= a11*a22*...*akk,
     * thus we approximate det(hess) with a11*a22*...*akk.
     *
     * @param sumGrad sum of gradient values
     * @param sumHess sum of hessian values
     * @return true if satisfied, false otherwise
     */
    public boolean satisfyWeight(double[] sumGrad, double[] sumHess) {
        return !MathUtil.areZeros(sumGrad) && satisfyWeight(sumHess);
    }

    /**
     * Calculate leaf weight given the statistics for binary classification.
     *
     * @param sumGrad sum of gradient values
     * @param sumHess sum of hessian values
     * @return weight
     */
    public double calcWeight(double sumGrad, double sumHess) {
        if (!satisfyWeight(sumHess) || sumGrad == 0.0) {
            return 0.0;
        }
        double dw;
        if (regAlpha == 0.0f) {
            dw = -sumGrad / (sumHess + regLambda);
        } else {
            dw = -MathUtil.thresholdL1(sumGrad, regAlpha) / (sumHess + regLambda);
        }
        if (maxLeafWeight != 0.0f) {
            if (dw > maxLeafWeight) {
                dw = maxLeafWeight;
            } else if (dw < -maxLeafWeight) {
                dw = -maxLeafWeight;
            }
        }
        return dw;
    }

    /**
     * Calculate leaf weights given the statistics for multi classification.
     *
     * @param sumGrad sum of gradient values
     * @param sumHess sum of hessian values
     * @return weight
     */
    public double[] calcWeights(double[] sumGrad, double[] sumHess) {
        double[] weights = new double[numClass];
        if (!satisfyWeight(sumHess) || MathUtil.areZeros(sumGrad)) {
            return weights;
        }
        // TODO: regularization
        if (!fullHessian) {
            if (regAlpha == 0.0f) {
                for (int k = 0; k < numClass; k++)
                    weights[k] = -sumGrad[k] / (sumHess[k] + regLambda);
            } else {
                for (int k = 0; k < numClass; k++)
                    weights[k] = -MathUtil.thresholdL1(sumGrad[k], regAlpha) / (sumHess[k] + regLambda);
            }
        } else {
            MathUtil.addDiagonal(numClass, sumHess, regLambda);
            weights = MathUtil.solveLinearSystemWithCholeskyDecomposition(sumHess, sumGrad, numClass);
            for (int i = 0; i < numClass; i++)
                weights[i] *= -1;
            MathUtil.addDiagonal(numClass, sumHess, -regLambda);
        }
        if (maxLeafWeight != 0.0f) {
            for (int k = 0; k < numClass; k++) {
                if (weights[k] > maxLeafWeight) {
                    weights[k] = maxLeafWeight;
                } else if (weights[k] < -maxLeafWeight) {
                    weights[k] = -maxLeafWeight;
                }
            }
        }
        return weights;
    }

    /**
     * Calculate the cost of loss function for binary classification.
     *
     * @param sumGrad sum of gradient values
     * @param sumHess sum of hessian values
     * @return loss gain
     */
    public double calcGain(double sumGrad, double sumHess) {
        if (!satisfyWeight(sumHess) || sumGrad == 0.0f) {
            return 0.0f;
        }
        if (maxLeafWeight == 0.0f) {
            if (regAlpha == 0.0f) {
                return (sumGrad / (sumHess + regLambda)) * sumGrad;
            } else {
                return MathUtil.sqr(MathUtil.thresholdL1(sumGrad, regAlpha)) / (sumHess + regLambda);
            }
        } else {
            double w = calcWeight(sumGrad, sumHess);
            double ret = sumGrad * w + 0.5 * (sumHess + regLambda) * MathUtil.sqr(w);
            if (regAlpha == 0.0f) {
                return -2.0 * ret;
            } else {
                return -2.0 * (ret + regAlpha * Math.abs(w));
            }
        }
    }

    /**
     * Calculate the cost of loss function for multi classification.
     *
     * @param sumGrad sum of gradient values
     * @param sumHess sum of hessian values
     * @return loss gain
     */
    public double calcGain(double[] sumGrad, double[] sumHess) {
        double gain = 0.0;
        if (!satisfyWeight(sumHess) || MathUtil.areZeros(sumGrad)) {
            return 0.0;
        }
        // TODO: regularization
        if (!fullHessian) {
            if (regAlpha == 0.0f) {
                for (int k = 0; k < numClass; k++)
                    gain += sumGrad[k] / (sumHess[k] + regLambda) * sumGrad[k];
            } else {
                for (int k = 0; k < numClass; k++)
                    gain += MathUtil.sqr(MathUtil.thresholdL1(sumGrad[k], regAlpha)) * (sumHess[k] + regLambda);
            }
        } else {
            MathUtil.addDiagonal(numClass, sumHess, regLambda);
            double[] tmp = MathUtil.solveLinearSystemWithCholeskyDecomposition(sumHess, sumGrad, numClass);
            gain = MathUtil.dot(sumGrad, tmp);
            MathUtil.addDiagonal(numClass, sumHess, -regLambda);
        }
        return (float) (gain / numClass);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(regTParam.toString());
        sb.append(String.format("|isRegression = %s\n", isRegression));
        sb.append(String.format("|numClass = %d\n", numClass));
        sb.append(String.format("|numRound = %d\n", numRound));
        sb.append(String.format("|initLearningRate = %f\n", initLearningRate));
        sb.append(String.format("|minChildWeight = %f\n", minChildWeight));
        sb.append(String.format("|regAlpha = %s\n", regAlpha));
        sb.append(String.format("|regLambda = %s\n", regLambda));
        sb.append(String.format("|maxLeafWeight = %s\n", maxLeafWeight));
        sb.append(String.format("|fullHessian = %s\n", fullHessian));
        sb.append(String.format("|multiTree = %s\n", multiTree));
        sb.append(String.format("|lossFunc = %s\n", lossFunc));
        sb.append(String.format("|evalMetrics = %s\n", Arrays.toString(evalMetrics)));
        return sb.toString();
    }

    /** -------------------- Default hyper-parameters -------------------- */
    public static final boolean DEFAULT_IS_REGRESSION = false;
    public static final int DEFAULT_NUM_CLASS = 2;
    public static final int DEFAULT_NUM_ROUND = 20;
    public static final float DEFAULT_INIT_LEARNING_RATE = 0.1f;
    public static final float DEFAULT_MIN_CHILD_WEIGHT = 0.0f;
    public static final float DEFAULT_REG_ALPHA = 0.0f;
    public static final float DEFAULT_REG_LAMBDA = 1.0f;
    public static final float DEFAULT_MAX_LEAF_WEIGHT = 0.0f;
    public static final boolean DEFAULT_FULL_HESSIAN = false;
    public static final boolean DEFAULT_MULTI_TREE = false;
}
