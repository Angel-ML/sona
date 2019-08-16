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
package org.apache.spark.angel.ml.tree.basic.split;

import java.io.Serializable;

public abstract class SplitEntry implements Serializable {
    protected int fid;
    protected float gain;

    public SplitEntry(int fid, float gain) {
        this.fid = fid;
        this.gain = gain;
    }

    public SplitEntry() {
        this(-1, 0.0f);
    }

    public boolean isEmpty() {
        return fid == -1;
    }

    public abstract int flowTo(float x);

    public abstract int defaultTo();

    public boolean needReplace(float newGain) {
        return gain < newGain;
    }

    public boolean needReplace(SplitEntry e) {
        return !compareTo(this, e);
    }

    /**
     * Comparison between two split entries.
     *
     * @param e1 first entry
     * @param e2 second entry
     * @return true if e1 is better than e2, false otherwise.
     */
    private static boolean compareTo(SplitEntry e1, SplitEntry e2) {
        if (e1.gain < e2.gain) {
            return false;
        } else if (e1.gain > e2.gain) {
            return true;
        } else {
            SplitType t1 = e1.splitType(), t2 = e2.splitType();
            if (t1 != t2) {
                // in order to reduce model size, we give priority to split point
                return t1.compareTo(t2) <= 0;
            } else if (t1 == SplitType.SPLIT_POINT) {
                // comparison between two split points, we give priority to lower feature index
                return e1.fid <= e2.fid;
            } else {
                // comparison between two split sets, we prefer fewer edges
                return ((SplitSet) e1).getEdges().length <= ((SplitSet) e2).getEdges().length;
            }
        }
    }

    public int getFid() {
        return fid;
    }

    public void setFid(int fid) {
        this.fid = fid;
    }

    public float getGain() {
        return gain;
    }

    public void setGain(float gain) {
        this.gain = gain;
    }

    public abstract SplitType splitType();

    @Override
    public abstract String toString();
}
