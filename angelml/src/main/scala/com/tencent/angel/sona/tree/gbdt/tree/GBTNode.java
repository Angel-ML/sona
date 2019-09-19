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
package com.tencent.angel.sona.tree.gbdt.tree;

import com.tencent.angel.sona.tree.gbdt.histogram.GradPair;
import com.tencent.angel.sona.tree.regression.RegTNode;

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

