package org.apache.spark.angel.ml.tree.regression;

import org.apache.spark.angel.ml.tree.basic.TNode;

public abstract class RegTNode extends TNode {
    protected float gain;
    protected float[] weights;

    public RegTNode(int nid, RegTNode parent, RegTNode leftChild, RegTNode rightChild) {
        super(nid, parent, leftChild, rightChild);
    }

    public RegTNode(int nid, RegTNode parent) {
        this(nid, parent, null, null);
    }

    public float getGain() {
        return gain;
    }

    public float getWeight() {
        return weights[0];
    }

    public float[] getWeights() {
        return weights;
    }
}
