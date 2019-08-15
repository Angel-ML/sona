package org.apache.spark.angel.ml.tree.basic;

import java.io.Serializable;
import java.util.Map;
import java.util.TreeMap;

public abstract class Tree<Param extends TParam, Node extends TNode> implements Serializable {
    protected final Param param;
    protected Map<Integer, Node> nodes;

    public Tree(Param param) {
        this.param = param;
        this.nodes = new TreeMap<>();
    }

    public Param getParam() {
        return this.param;
    }

    public Node getRoot() {
        return this.nodes.get(0);
    }

    public void setRoot(Node root) {
        if (root.getParent() != null)
            throw new RuntimeException("Root node should not have parent");
        setNode(0, root);
    }

    public Node getNode(int nid) {
        return this.nodes.get(nid);
    }

    public void setNode(int nid, Node node) {
        this.nodes.put(nid, node);
    }

    public Map<Integer, Node> getNodes() {
        return this.nodes;
    }

    public int size() {
        return nodes.size();
    }

    public int numLeaves() {
        return (nodes.size() + 1) / 2;
    }
}
