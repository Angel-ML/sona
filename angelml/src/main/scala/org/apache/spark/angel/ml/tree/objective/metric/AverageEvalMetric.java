package org.apache.spark.angel.ml.tree.objective.metric;

public abstract class AverageEvalMetric implements EvalMetric {

    @Override
    public double eval(float[] preds, float[] labels) {
        return eval(preds, labels, 0, labels.length);
    }

    @Override
    public double eval(float[] preds, float[] labels, int start, int end) {
        if (preds.length == labels.length) {
            double res = 0.0;
            for (int i = start; i < end; i++)
                res += evalOne(preds[i], labels[i]);
            return res / (end - start);
        } else if (preds.length % labels.length == 0) {
            double res = 0.0;
            int numLabel = preds.length / labels.length;
            float[] pred = new float[numLabel];
            for (int i = start; i < end; i++) {
                System.arraycopy(preds, i * numLabel, pred, 0, numLabel);
                res += evalOne(pred, labels[i]);
            }
            return res / (end - start);
        } else {
            throw new RuntimeException(String.format("Got %d predictions given %d labels",
                    preds.length, labels.length));
        }
    }

    public double evalOne(float pred, float label) {
        throw new RuntimeException(this.getKind() + " does not support binary-label");
    }

    public double evalOne(float[] pred, float label) {
        throw new RuntimeException(this.getKind() + " does not support multi-label");
    }
}
