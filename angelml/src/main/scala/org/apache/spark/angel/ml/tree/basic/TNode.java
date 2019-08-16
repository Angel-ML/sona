package org.apache.spark.angel.ml.tree.basic;

import org.apache.spark.angel.ml.tree.basic.split.SplitEntry;

import java.io.Serializable;

/**
 * The abstract class for tree node.
 *
 */
public abstract class TNode implements Serializable {
    /**
     * Metadata for tree node. Tree node index starts from 0.
     */
    private final int nid;
    private final TNode parent;
    private TNode leftChild;
    private TNode rightChild;
    private SplitEntry splitEntry;
    private boolean isLeaf;

    public TNode(int nid, TNode parent, TNode leftChild, TNode rightChild) {
        this.nid = nid;
        this.parent = parent;
        this.leftChild = leftChild;
        this.rightChild = rightChild;
        this.isLeaf = false;
    }

    public TNode(int nid, TNode parent) {
        this(nid, parent, null, null);
    }

    public int getNid() {
        return nid;
    }

    public TNode getParent() {
        return parent;
    }

    public TNode getLeftChild() {
        return leftChild;
    }

    public void setLeftChild(TNode leftChild) {
        this.leftChild = leftChild;
    }

    public TNode getRightChild() {
        return rightChild;
    }

    public void setRightChild(TNode rightChild) {
        this.rightChild = rightChild;
    }

    public SplitEntry getSplitEntry() {
        return splitEntry;
    }

    public void setSplitEntry(SplitEntry splitEntry) {
        this.splitEntry = splitEntry;
    }

    public boolean isLeaf() {
        return isLeaf;
    }

    public void chgToLeaf() {
        isLeaf = true;
    }

}
