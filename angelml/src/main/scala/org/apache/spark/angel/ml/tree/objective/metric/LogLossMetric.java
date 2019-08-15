package org.apache.spark.angel.ml.tree.objective.metric;

import org.apache.spark.angel.ml.tree.util.MathUtil;

import javax.inject.Singleton;

@Singleton
public class LogLossMetric extends AverageEvalMetric {
    private static LogLossMetric instance;

    private LogLossMetric() {}

    @Override
    public Kind getKind() {
        return Kind.LOG_LOSS;
    }

    @Override
    public double evalOne(float pred, float label) {
        float prob = MathUtil.fastSigmoid(pred);
        return -(label * MathUtil.fastLog(prob) + (1 - label) * MathUtil.fastLog(1 - prob));
    }

    public static LogLossMetric getInstance() {
        if (instance == null)
            instance = new LogLossMetric();
        return instance;
    }
}
