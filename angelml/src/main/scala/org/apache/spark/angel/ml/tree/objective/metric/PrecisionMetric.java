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

import javax.inject.Singleton;

@Singleton
public class PrecisionMetric extends AverageEvalMetric {
    private static PrecisionMetric instance;

    private PrecisionMetric() {}

    @Override
    public Kind getKind() {
        return Kind.PRECISION;
    }

    @Override
    public double evalOne(float pred, float label) {
        return pred < 0.0f ? 1.0 - label : label;
    }

    @Override
    public double evalOne(float[] pred, float label) {
        return MathUtil.argmax(pred) == ((int) label) ? 1 : 0;
    }

    public static PrecisionMetric getInstance() {
        if (instance == null)
            instance = new PrecisionMetric();
        return instance;
    }
}