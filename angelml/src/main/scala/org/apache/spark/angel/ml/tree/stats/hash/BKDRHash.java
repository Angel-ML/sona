package org.apache.spark.angel.ml.tree.stats.hash;

public class BKDRHash extends Int2IntHash {
    private int seed;

    public BKDRHash(int size, int seed) {
        super(size);
        this.seed = seed;
    }

    protected int doHash(int key) {
        int code = 0;
        while (key != 0) {
            code = seed * code + (key % 10);
            key /= 10;
        }
        return code;
    }

    @Override
    public Int2IntHash clone() {
        BKDRHash copy = new BKDRHash(size, seed);
        copy.buffer = this.buffer;
        return copy;
    }

    @Override
    public boolean equalsIgnoreSize(Int2IntHash other) {
        return other instanceof BKDRHash
                && ((BKDRHash) other).getSeed() == seed;
    }

    public int getSeed() {
        return seed;
    }
}
