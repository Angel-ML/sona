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

import org.apache.spark.angel.ml.tree.basic.TParam;

public class RegTParam extends TParam {
    public boolean leafwise = DEFAULT_LEAFWISE;
    public float minSplitGain = DEFAULT_MIN_SPLIT_GAIN;
    public int minNodeInstance = DEFAULT_MIN_NODE_INSTANCE;

    public RegTParam setLeafwise(boolean leafwise) {
        this.leafwise = leafwise;
        return this;
    }

    public RegTParam setMinSplitGain(float minSplitGain) {
        this.minSplitGain = minSplitGain;
        return this;
    }

    public RegTParam setMinNodeInstance(int minNodeInstance) {
        this.minNodeInstance = minNodeInstance;
        return this;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(super.toString());
        sb.append(String.format("|leafwise = %s\n", leafwise));
        sb.append(String.format("|minSplitGain = %f\n", minSplitGain));
        sb.append(String.format("|minNodeInstance = %d\n", minNodeInstance));
        return sb.toString();
    }

    /** -------------------- Default hyper-parameters -------------------- */
    public static final boolean DEFAULT_LEAFWISE = false;
    public static final float DEFAULT_MIN_SPLIT_GAIN = 0.0f;
    public static final int DEFAULT_MIN_NODE_INSTANCE = 1024;
}
