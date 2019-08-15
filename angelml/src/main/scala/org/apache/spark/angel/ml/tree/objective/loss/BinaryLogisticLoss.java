package org.apache.spark.angel.ml.tree.objective.loss;

import org.apache.spark.angel.ml.tree.objective.metric.EvalMetric;
import org.apache.spark.angel.ml.tree.util.MathUtil;

import javax.inject.Singleton;

@Singleton
public class BinaryLogisticLoss implements BinaryLoss {
    private static BinaryLogisticLoss instance;

    private BinaryLogisticLoss() {}

    @Override
    public Kind getKind() {
        return Kind.BinaryLogistic;
    }

    @Override
    public EvalMetric.Kind defaultEvalMetric() {
        return EvalMetric.Kind.LOG_LOSS;
    }

    @Override
    public double firOrderGrad(float pred, float label) {
        double prob = MathUtil.fastSigmoid(pred);
        return prob - label;
    }

    @Override
    public double secOrderGrad(float pred, float label) {
        double prob = MathUtil.fastSigmoid(pred);
        return Math.max(prob * (1 - prob), MathUtil.EPSILON);
    }

    @Override
    public double secOrderGrad(float pred, float label, double firGrad) {
        double prob = firGrad + label;
        return Math.max(prob * (1 - prob), MathUtil.EPSILON);
    }

    public static BinaryLogisticLoss getInstance() {
        if (instance == null)
            instance = new BinaryLogisticLoss();
        return instance;
    }
}
