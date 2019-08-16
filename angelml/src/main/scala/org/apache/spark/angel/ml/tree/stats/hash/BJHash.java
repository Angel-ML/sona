package org.apache.spark.angel.ml.tree.stats.hash;

public class BJHash extends Int2IntHash {
    public BJHash(int size) {
        super(size);
    }

    protected int doHash(int key) {
        int code = key;
        code = (code + 0x7ed55d16) + (code << 12);
        code = (code ^ 0xc761c23c) ^ (code >> 19);
        code = (code + 0x165667b1) + (code << 5);
        code = (code + 0xd3a2646c) ^ (code << 9);
        code = (code + 0xfd7046c5) + (code << 3);
        code = (code ^ 0xb55a4f09) ^ (code >> 16);
        return code;
    }

    @Override
    public Int2IntHash clone() {
        BJHash copy = new BJHash(size);
        copy.buffer = this.buffer;
        return copy;
    }

    @Override
    public boolean equalsIgnoreSize(Int2IntHash other) {
        return other instanceof BJHash;
    }
}
