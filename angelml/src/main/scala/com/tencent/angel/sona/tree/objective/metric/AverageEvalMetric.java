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
package com.tencent.angel.sona.tree.objective.metric;

public abstract class AverageEvalMetric implements EvalMetric {

    @Override
    public double eval(float[] preds, float[] labels) {
        return eval(preds, labels, 0, labels.length);
    }

    @Override
    public double eval(float[] preds, float[] labels, int start, int end) {
        if (preds.length == labels.length) {
            double res = 0.0;
            for (int i = start; i < end; i++)
                res += evalOne(preds[i], labels[i]);
            return res / (end - start);
        } else if (preds.length % labels.length == 0) {
            double res = 0.0;
            int numLabel = preds.length / labels.length;
            float[] pred = new float[numLabel];
            for (int i = start; i < end; i++) {
                System.arraycopy(preds, i * numLabel, pred, 0, numLabel);
                res += evalOne(pred, labels[i]);
            }
            return res / (end - start);
        } else {
            throw new RuntimeException(String.format("Got %d predictions given %d labels",
                    preds.length, labels.length));
        }
    }

    public double evalOne(float pred, float label) {
        throw new RuntimeException(this.getKind() + " does not support binary-label");
    }

    public double evalOne(float[] pred, float label) {
        throw new RuntimeException(this.getKind() + " does not support multi-label");
    }
}
