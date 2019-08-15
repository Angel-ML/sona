package org.apache.spark.angel.ml.tree.gbdt.histogram;

import org.apache.spark.angel.ml.tree.gbdt.tree.GBDTParam;

public interface GradPair {
    void plusBy(GradPair gradPair);

    void subtractBy(GradPair gradPair);

    GradPair plus(GradPair gradPair);

    GradPair subtract(GradPair gradPair);

    void timesBy(double x);

    boolean satisfyWeight(GBDTParam param);

    float calcGain(GBDTParam param);

    public float calcWeight(GBDTParam param);

    public float[] calcWeights(GBDTParam param);

    GradPair copy();

    void clear();
}
