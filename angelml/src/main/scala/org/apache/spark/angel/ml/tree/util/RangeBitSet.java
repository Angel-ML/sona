package org.apache.spark.angel.ml.tree.util;

import java.io.Serializable;
import java.util.List;

public class RangeBitSet implements Serializable {
    private byte[] bits;
    private int from;
    private int to;
    private int offset;

    public RangeBitSet(int from, int to) {
        this.from = from;
        this.to = to;
        this.offset = from & 0b111;
        this.bits = new byte[needNumBytes(from, to)];
    }

    public RangeBitSet(int from, int to, byte[] bits) {
        this.from = from;
        this.to = to;
        this.offset = from & 0b111;
        if (bits.length != needNumBytes(from, to)) {
            throw new RuntimeException(String.format("Invalid RangeBitSet size: " +
                    "%d, should be %d", bits.length, needNumBytes(from, to)));
        } else {
            this.bits = bits;
        }
    }

    public RangeBitSet() {
        this.from = -1;
        this.to = -1;
        this.offset = -1;
        this.bits = null;
    }

    private int needNumBytes(int from, int to) {
        int first = from >> 3;
        int last = to >> 3;
        return last - first + 1;
    }

    public void set(int index) {
        index = index - from + offset;
        int x = index >> 3;
        int y = index & 0b111;
        bits[x] = (byte) (bits[x] | (1 << y));
    }

    public void clear(int index) {
        index = index - from + offset;
        int x = index >> 3;
        int y = index & 0b111;
        bits[x] = (byte) (bits[x] & (~(1 << y)));
    }

    // TODO: use arraycopy to make it faster
    public void or(RangeBitSet other) {
        int from = other.getRangeFrom(), to = other.getRangeTo();
        // assert from >= this.from && to <= this.to;
        for (int i = from; i <= to; i++) {
            if (other.get(i)) {
                set(i);
            }
        }
    }

    public boolean get(int index) {
        index = index - from + offset;
        int x = index >> 3;
        int y = index & 0b111;
        return ((bits[x] >> y) & 0x1) == 1;
    }

    public RangeBitSet subset(int newFrom, int newTo) {
        if ((newFrom <= from && newTo >= to) || newFrom > newTo) {
            throw new RuntimeException(String.format("Invalid subset range: [%d-%d], " +
                    "should be in [%d-%d]", newFrom, newTo, from, to));
        }
        int firstByteIdx = (newFrom - from) >> 3;
        int lastByteIdx = (newTo - from) >> 3;
        int numBytes = lastByteIdx - firstByteIdx + 1;
        byte[] subset = new byte[numBytes];
        System.arraycopy(bits, firstByteIdx, subset, 0, numBytes);
        return new RangeBitSet(newFrom, newTo, subset);
    }

    public RangeBitSet overlap(int newFrom, int newTo) {
        newFrom = Math.max(newFrom, from);
        newTo = Math.min(newTo, to);
        if (newFrom > newTo) {
            return null;
        }
        if (newFrom != from || newTo != to) {
            return subset(newFrom, newTo);
        } else {
            return this;
        }
    }

    public byte[] toByteArray() {
        return bits;
    }

    public int getRangeFrom() {
        return from;
    }

    public int getRangeTo() {
        return to;
    }

    public int getNumValid() {
        int res = 0;
        for (int i = from; i <= to; i++) {
            if (get(i)) {
                res++;
            }
        }
        return res;
    }

    public static RangeBitSet or(RangeBitSet bs1, RangeBitSet bs2) {
        int from = Math.min(bs1.getRangeFrom(), bs2.getRangeFrom());
        int to = Math.max(bs1.getRangeTo(), bs2.getRangeTo());
        RangeBitSet res = new RangeBitSet(from, to);
        res.or(bs1);
        res.or(bs2);
        return res;
    }

    public static RangeBitSet or(List<RangeBitSet> bitsets) {
        int from = Integer.MAX_VALUE, to = Integer.MIN_VALUE;
        int size = bitsets.size();
        for (int i = 0; i < size; i++) {
            from = Math.min(bitsets.get(i).getRangeFrom(), from);
            to = Math.max(bitsets.get(i).getRangeTo(), to);
        }
        RangeBitSet res = new RangeBitSet(from, to);
        for (int i = 0; i < size; i++) {
            res.or(bitsets.get(i));
        }
        return res;
    }
}
