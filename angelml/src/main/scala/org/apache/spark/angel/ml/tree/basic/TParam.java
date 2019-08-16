package org.apache.spark.angel.ml.tree.basic;

import org.apache.spark.angel.ml.tree.util.MathUtil;

import java.io.Serializable;

public abstract class TParam implements Serializable {
    public int numFeature;
    public int maxDepth = DEFAULT_MAX_DEPTH;
    public int maxNodeNum = DEFAULT_MAX_NODE_NUM;
    public int numSplit = DEFAULT_NUM_SPLIT;
    public float insSampleRatio = DEFAULT_INS_SAMPLE_RATIO;
    public float featSampleRatio = DEFAULT_FEAT_SAMPLE_RATIO;

    public TParam setNumFeature(int numFeature) {
        this.numFeature = numFeature;
        return this;
    }

    public TParam setMaxDepth(int maxDepth) {
        this.maxDepth = maxDepth;
        this.maxNodeNum = Math.min(this.maxNodeNum, MathUtil.maxNodeNum(maxDepth));
        return this;
    }

    public TParam setMaxNodeNum(int maxNodeNum) {
        this.maxNodeNum = Math.min(maxNodeNum, MathUtil.maxNodeNum(maxDepth));
        return this;
    }

    public TParam setNumSplit(int numSplit) {
        this.numSplit = Math.min(numSplit, 255);
        return this;
    }

    public TParam setInsSampleRatio(float insSampleRatio) {
        this.insSampleRatio = Math.min(insSampleRatio, 1.0f);
        return this;
    }

    public TParam setFeatSampleRatio(float featSampleRatio) {
        this.featSampleRatio = Math.min(featSampleRatio, 1.0f);
        return this;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("|numFeature = %d\n", numFeature));
        sb.append(String.format("|maxDepth = %d\n", maxDepth));
        sb.append(String.format("|maxNodeNum = %d\n", maxNodeNum));
        sb.append(String.format("|numSplit = %d\n", numSplit));
        sb.append(String.format("|insSampleRatio = %f\n", insSampleRatio));
        sb.append(String.format("|featSampleRatio = %f\n", featSampleRatio));
        return sb.toString();
    }

    /** -------------------- Default hyper-parameters -------------------- */
    public static final int DEFAULT_MAX_DEPTH = 6;
    public static final int DEFAULT_MAX_NODE_NUM = MathUtil.maxNodeNum(DEFAULT_MAX_DEPTH);
    public static final int DEFAULT_NUM_SPLIT = 20;
    public static final float DEFAULT_INS_SAMPLE_RATIO = 1.0f;
    public static final float DEFAULT_FEAT_SAMPLE_RATIO = 1.0f;
}
