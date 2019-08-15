package org.apache.spark.angel.ml.tree.stats.hash;

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
