package org.apache.spark.angel.ml.tree.regression;

import org.apache.spark.angel.ml.tree.basic.Tree;
import org.apache.spark.angel.ml.tree.basic.split.SplitEntry;
import org.apache.spark.ml.linalg.*;

import java.util.Arrays;

public class RegTree<Node extends RegTNode> extends Tree<RegTParam, Node> {

    public RegTree(RegTParam param) {
        super(param);
    }

    public Node flowToLeaf(Vector ins) {
        Node node = getRoot();
        while (!node.isLeaf()) {
            SplitEntry splitEntry = node.getSplitEntry();
            int fid = splitEntry.getFid();
            int flowTo;
            if (ins instanceof DenseVector) {
                DenseVector dv = (DenseVector) ins;
                flowTo = splitEntry.flowTo((float) dv.values()[fid]);
            } else {
                SparseVector sv = (SparseVector) ins;
                int t = Arrays.binarySearch(sv.indices(), fid);
                if (t >= 0)
                    flowTo = splitEntry.flowTo((float) sv.values()[t]);
                else
                    flowTo = splitEntry.defaultTo();
            }
            node = (Node) (flowTo == 0 ? node.getLeftChild() : node.getRightChild());
        }
        return node;
    }

    public float predictBinary(Vector ins) {
        return flowToLeaf(ins).getWeight();
    }

    public float[] predictMulti(Vector ins) {
        return flowToLeaf(ins).getWeights();
    }
}
