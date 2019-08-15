package org.apache.spark.angel.ml.tree.objective.metric;

import org.apache.spark.angel.ml.tree.util.MathUtil;

public class CrossEntropyMetric extends AverageEvalMetric {
    private static CrossEntropyMetric instance;

    private CrossEntropyMetric() {}

    @Override
    public Kind getKind() {
        return Kind.CROSS_ENTROPY;
    }

    @Override
    public double evalOne(float[] pred, float label) {
        double sum = 0.0;
        for (float p : pred) {
            sum += Math.exp(p);
        }
        double p = Math.exp(pred[(int) label]) / sum;
        return -MathUtil.fastLog(Math.max(p, MathUtil.EPSILON));
    }

    public static CrossEntropyMetric getInstance() {
        if (instance == null)
            instance = new CrossEntropyMetric();
        return instance;
    }
}
