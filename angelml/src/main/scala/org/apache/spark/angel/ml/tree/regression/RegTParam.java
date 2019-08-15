package org.apache.spark.angel.ml.tree.regression;

import org.apache.spark.angel.ml.tree.basic.TParam;

public class RegTParam extends TParam {
    public boolean leafwise = DEFAULT_LEAFWISE;
    public float minSplitGain = DEFAULT_MIN_SPLIT_GAIN;
    public int minNodeInstance = DEFAULT_MIN_NODE_INSTANCE;

    public RegTParam setLeafwise(boolean leafwise) {
        this.leafwise = leafwise;
        return this;
    }

    public RegTParam setMinSplitGain(float minSplitGain) {
        this.minSplitGain = minSplitGain;
        return this;
    }

    public RegTParam setMinNodeInstance(int minNodeInstance) {
        this.minNodeInstance = minNodeInstance;
        return this;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(super.toString());
        sb.append(String.format("|leafwise = %s\n", leafwise));
        sb.append(String.format("|minSplitGain = %f\n", minSplitGain));
        sb.append(String.format("|minNodeInstance = %d\n", minNodeInstance));
        return sb.toString();
    }

    /** -------------------- Default hyper-parameters -------------------- */
    public static final boolean DEFAULT_LEAFWISE = false;
    public static final float DEFAULT_MIN_SPLIT_GAIN = 0.0f;
    public static final int DEFAULT_MIN_NODE_INSTANCE = 1024;
}
