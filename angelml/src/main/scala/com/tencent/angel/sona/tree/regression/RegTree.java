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
package com.tencent.angel.sona.tree.regression;

import com.tencent.angel.sona.tree.basic.Tree;
import com.tencent.angel.sona.tree.basic.split.SplitEntry;
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
