package org.apache.spark.angel.ml.tree.objective.loss;

public interface BinaryLoss extends Loss {
    double firOrderGrad(float pred, float label);

    double secOrderGrad(float pred, float label);

    double secOrderGrad(float pred, float label, double firGrad);
}
