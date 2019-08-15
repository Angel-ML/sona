package org.apache.spark.angel.ml.tree.objective.metric;

public interface EvalMetric {
    Kind getKind();

    double eval(float[] preds, float[] labels);

    double eval(float[] preds, float[] labels, int start, int end);

    public enum Kind {
        RMSE("rmse"),
        ERROR("error"),
        LOG_LOSS("log-loss"),
        CROSS_ENTROPY("cross-entropy"),
        PRECISION("precision"),
        AUC("auc");

        private final String kind;

        private Kind(String kind) {
            this.kind = kind;
        }

        @Override
        public String toString() {
            return kind;
        }
    }
}
