package org.apache.spark.angel.ml.tree.basic.split;

public class SplitPoint extends SplitEntry {
    private float fvalue;

    public SplitPoint() {
        this(-1, 0.0f, 0.0f);
    }

    public SplitPoint(int fid, float fvalue, float gain) {
        super(fid, gain);
        this.fvalue = fvalue;
    }

    @Override
    public int flowTo(float x) {
        return x < fvalue ? 0 : 1;
    }

    @Override
    public int defaultTo() {
        return fvalue > 0.0f ? 0 : 1;
    }

    @Override
    public SplitType splitType() {
        return SplitType.SPLIT_POINT;
    }

    public float getFvalue() {
        return fvalue;
    }

    public void setFvalue(float fvalue) {
        this.fvalue = fvalue;
    }

    @Override
    public String toString() {
        return String.format("%s fid[%d] fvalue[%f] gain[%f]",
                this.splitType(), fid, fvalue, gain);
    }
}
