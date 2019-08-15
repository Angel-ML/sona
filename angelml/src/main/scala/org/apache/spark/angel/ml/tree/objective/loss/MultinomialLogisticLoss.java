package org.apache.spark.angel.ml.tree.objective.loss;

import org.apache.spark.angel.ml.tree.objective.metric.EvalMetric;
import org.apache.spark.angel.ml.tree.util.MathUtil;

import javax.inject.Singleton;

@Singleton
public class MultinomialLogisticLoss implements MultiLoss {
    private static MultinomialLogisticLoss instance;

    private MultinomialLogisticLoss() {}

    @Override
    public Kind getKind() {
        return Kind.MultiLogistic;
    }

    @Override
    public EvalMetric.Kind defaultEvalMetric() {
        return EvalMetric.Kind.CROSS_ENTROPY;
    }

    @Override
    public double[] firOrderGrad(float[] pred, float label) {
        double[] prob = MathUtil.floatArrayToDoubleArray(pred);
        MathUtil.softmax(prob);
        int trueLabel = (int) label;
        double[] grad = prob;
        for (int i = 0; i < grad.length; i++) {
            grad[i] = (trueLabel == i ? prob[i] - 1.0 : prob[i]);
        }
        return grad;
    }

    @Override
    public double[] secOrderGradDiag(float[] pred, float label) {
        double[] prob = MathUtil.floatArrayToDoubleArray(pred);
        MathUtil.softmax(prob);
        double[] hess = prob;
        for (int i = 0; i < hess.length; i++) {
            hess[i] = Math.max(prob[i] * (1.0f - prob[i]), MathUtil.EPSILON);
        }
        return hess;
    }

    @Override
    public double[] secOrderGradDiag(float[] pred, float label, double[] firGrad) {
        int trueLabel = (int) label;
        double[] hess = new double[pred.length];
        for (int i = 0; i < hess.length; i++) {
            double prob = trueLabel == i ? firGrad[i] + 1.0 : firGrad[i];
            hess[i] = Math.max(prob * (1.0 - prob), MathUtil.EPSILON);
        }
        return hess;
    }

    @Override
    public double[] secOrderGradFull(float[] pred, float label) {
        double[] prob = MathUtil.floatArrayToDoubleArray(pred);
        MathUtil.softmax(prob);
        int numLabel = pred.length;
        double[] hess = new double[numLabel * (numLabel + 1) / 2];
        for (int i = 0; i < numLabel; i++) {
            int rowI = MathUtil.indexOfLowerTriangularMatrix(i, 0);
            for (int j = 0; j < i; j++) {
                hess[rowI + j] = Math.min(-prob[i] * prob[j], -MathUtil.EPSILON);
            }
            hess[rowI + i] = Math.max(prob[i] * (1.0 - prob[i]), MathUtil.EPSILON);
        }
        return hess;
    }

    @Override
    public double[] secOrderGradFull(float[] pred, float label, double[] firGrad) {
        int numLabel = pred.length;
        int trueLabel = (int) label;
        double[] prob = new double[numLabel];
        for (int i = 0; i < numLabel; i++)
            prob[i] = trueLabel == i ? firGrad[i] + 1.0 : firGrad[i];
        double[] hess = new double[numLabel * (numLabel + 1) / 2];
        for (int i = 0; i < numLabel; i++) {
            int rowI = MathUtil.indexOfLowerTriangularMatrix(i, 0);
            for (int j = 0; j < i; j++) {
                hess[rowI + j] = Math.min(-prob[i] * prob[j], -MathUtil.EPSILON);
            }
            hess[rowI + i] = Math.max(prob[i] * (1.0 - prob[i]), MathUtil.EPSILON);
        }
        return hess;
    }

    public static MultinomialLogisticLoss getInstance() {
        if (instance == null)
            instance = new MultinomialLogisticLoss();
        return instance;
    }
}

