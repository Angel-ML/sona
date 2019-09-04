/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017-2018 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */
package com.tencent.angel.sona.tree.basic;

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
