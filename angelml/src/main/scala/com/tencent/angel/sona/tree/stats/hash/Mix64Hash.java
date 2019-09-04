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
package com.tencent.angel.sona.tree.stats.hash;

public class Mix64Hash extends Int2IntHash {
    public Mix64Hash(int size) {
        super(size);
    }

    protected int doHash(int key) {
        int code = key;
        code = (~code) + (code << 21); // code = (code << 21) - code - 1;
        code = code ^ (code >> 24);
        code = (code + (code << 3)) + (code << 8); // code * 265
        code = code ^ (code >> 14);
        code = (code + (code << 2)) + (code << 4); // code * 21
        code = code ^ (code >> 28);
        code = code + (code << 31);
        return code;
    }

    @Override
    public Int2IntHash clone() {
        Mix64Hash copy = new Mix64Hash(size);
        copy.buffer = this.buffer;
        return copy;
    }

    @Override
    public boolean equalsIgnoreSize(Int2IntHash other) {
        return other instanceof Mix64Hash;
    }
}
