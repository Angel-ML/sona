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
package org.apache.spark.angel.ml.tree.stats.hash;

import java.io.Serializable;

public abstract class Int2IntHash implements Serializable {
    protected int size;

    public Int2IntHash(int size) {
        this.size = size;
    }

    public int hash(int key) {
        int code;
        if (buffer != null) {
            code = buffer[key];
        } else {
            code = doHash(key);
        }
        code %= size;
        return code >= 0 ? code : code + size;
    }

    protected abstract int doHash(int key);

    public abstract Int2IntHash clone();

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public abstract boolean equalsIgnoreSize(Int2IntHash other);

    protected transient int[] buffer;

    public void precompute(int maxKey) {
        if (buffer == null || buffer.length < maxKey) {
            buffer = new int[maxKey];
            for (int key = 0; key < maxKey; key++)
                buffer[key] = doHash(key);
        }
    }

    public int[] getPrecomputeBuffer() {
        return buffer;
    }

    public void setPrecomputeBuffer(int[] buffer) {
        this.buffer = buffer;
    }
}
