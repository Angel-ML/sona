package org.apache.spark.angel.ml.tree.objective.metric;

import javax.inject.Singleton;

@Singleton
public class RMSEMetric extends AverageEvalMetric {
    private static RMSEMetric instance;

    private RMSEMetric() {}

    @Override
    public Kind getKind() {
        return Kind.RMSE;
    }

    @Override
    public double evalOne(float pred, float label) {
        double diff = pred - label;
        return diff * diff;
    }

    @Override
    public double evalOne(float[] pred, float label) {
        double err = 0.0;
        int trueLabel = (int) label;
        for (int i = 0; i < pred.length; i++) {
            double diff = pred[i] - (i == trueLabel ? 1 : 0);
            err += diff * diff;
        }
        return err;
    }

    public static RMSEMetric getInstance() {
        if (instance == null)
            instance = new RMSEMetric();
        return instance;
    }
}
