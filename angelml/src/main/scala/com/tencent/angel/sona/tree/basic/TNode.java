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

import com.tencent.angel.sona.tree.basic.split.SplitEntry;

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
