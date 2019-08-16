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
