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
package org.apache.spark.angel.ml.tree.objective.metric;

import org.apache.spark.angel.ml.tree.util.MathUtil;

public class CrossEntropyMetric extends AverageEvalMetric {
    private static CrossEntropyMetric instance;

    private CrossEntropyMetric() {}

    @Override
    public Kind getKind() {
        return Kind.CROSS_ENTROPY;
    }

    @Override
    public double evalOne(float[] pred, float label) {
        double sum = 0.0;
        for (float p : pred) {
            sum += Math.exp(p);
        }
        double p = Math.exp(pred[(int) label]) / sum;
        return -MathUtil.fastLog(Math.max(p, MathUtil.EPSILON));
    }

    public static CrossEntropyMetric getInstance() {
        if (instance == null)
            instance = new CrossEntropyMetric();
        return instance;
    }
}
