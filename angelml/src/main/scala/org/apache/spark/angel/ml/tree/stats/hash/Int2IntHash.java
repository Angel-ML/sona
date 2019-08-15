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
