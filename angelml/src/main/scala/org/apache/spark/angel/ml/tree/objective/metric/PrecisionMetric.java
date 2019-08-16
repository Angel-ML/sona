package org.apache.spark.angel.ml.tree.objective.metric;

import org.apache.spark.angel.ml.tree.util.MathUtil;

import javax.inject.Singleton;

@Singleton
public class PrecisionMetric extends AverageEvalMetric {
    private static PrecisionMetric instance;

    private PrecisionMetric() {}

    @Override
    public Kind getKind() {
        return Kind.PRECISION;
    }

    @Override
    public double evalOne(float pred, float label) {
        return pred < 0.0f ? 1.0 - label : label;
    }

    @Override
    public double evalOne(float[] pred, float label) {
        return MathUtil.argmax(pred) == ((int) label) ? 1 : 0;
    }

    public static PrecisionMetric getInstance() {
        if (instance == null)
            instance = new PrecisionMetric();
        return instance;
    }
}