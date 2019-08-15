package org.apache.spark.angel.ml.tree.gbdt.tree;

import org.apache.spark.angel.ml.tree.gbdt.histogram.GradPair;
import org.apache.spark.angel.ml.tree.regression.RegTNode;

public class GBTNode extends RegTNode {
    private GradPair sumGradPair;

    public GBTNode(int nid, GBTNode parent, GBTNode leftChild, GBTNode rightChild) {
        super(nid, parent, leftChild, rightChild);
    }

    public GBTNode(int nid, GBTNode parent) {
        this(nid, parent, null, null);
    }

    public void setSumGradPair(GradPair sumGradPair) {
        this.sumGradPair = sumGradPair;
    }

    public float calcGain(GBDTParam param) {
        gain = sumGradPair.calcGain(param);
        return gain;
    }

    public float calcWeight(GBDTParam param) {
        if (weights == null)
            weights = new float[1];
        weights[0] = sumGradPair.calcWeight(param);
        return weights[0];
    }

    public float[] calcWeights(GBDTParam param) {
        weights = sumGradPair.calcWeights(param);
        return weights;
    }

    public GradPair getSumGradPair() {
        return sumGradPair;
    }
}

