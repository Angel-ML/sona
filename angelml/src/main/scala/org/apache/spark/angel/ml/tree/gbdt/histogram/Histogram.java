package org.apache.spark.angel.ml.tree.gbdt.histogram;

import java.io.Serializable;
import java.util.Arrays;

/**
 * The class for gradient histograms. A Histogram object contains
 * two histograms, one for gradient and one for hessian,
 * both for the same feature dimension.
 *
 * The accumulation of histogram requires numerical stability,
 * so we use double-precision floating points. instead of
 * single-precision floating points.
 */
public class Histogram implements Serializable {
    private int numBin;
    private int numClass;
    private boolean fullHessian;
    private double[] gradients;
    private double[] hessians;

    public Histogram(int numBin, int numClass, boolean fullHessian) {
        this.numBin = numBin;
        this.numClass = numClass;
        this.fullHessian = fullHessian;
        if (numClass == 2) {
            this.gradients = new double[numBin];
            this.hessians = new double[numBin];
        } else if (!fullHessian) {
            this.gradients = new double[numBin * numClass];
            this.hessians = new double[numBin * numClass];
        } else {
            this.gradients = new double[numBin * numClass];
            this.hessians = new double[numBin * ((numClass * (numClass + 1)) >> 1)];
        }
    }

    public Histogram(int numBin, int numClass, boolean fullHessian, double[] gradients, double[] hessians) {
        this.numBin = numBin;
        this.numClass = numClass;
        this.fullHessian = fullHessian;
        this.gradients = gradients;
        this.hessians = hessians;
    }

    /**
     * Accumulate a gradient value and a hessian value
     * to corresponding bin, for binary-classification.
     *
     * @param index bin index.
     * @param grad gradient value.
     * @param hess hessian value.
     */
    public void accumulate(int index, double grad, double hess) {
        gradients[index] += grad;
        hessians[index] += hess;
    }

    /**
     * Accumulate an array of gradients and hessians
     * to corresponding bin, for multi-classification.
     *
     * @param index bin index.
     * @param grad gradient values.
     * @param hess hessian values.
     */
    public void accumulate(int index, double[] grad, double[] hess) {
        if (!fullHessian) {
            accumulate(index, grad, hess, 0);
        } else {
            accumulate(index, grad, 0, hess, 0);
        }
    }

    /**
     * Accumulate an array of gradients and hessians
     * to corresponding bin given offset,
     * for multi-classification without full hessian.
     *
     * @param index bin index.
     * @param grad gradient values.
     * @param hess hessian values.
     * @param offset offset for both gradients and hessians.
     */
    public void accumulate(int index, double[] grad, double[] hess, int offset) {
        int binOffset = index * numClass;
        for (int i = 0; i < numClass; i++) {
            gradients[binOffset + i] += grad[offset + i];
            hessians[binOffset + i] += hess[offset + i];
        }
    }

    /**
     * Accumulate an array of gradients and hessians
     * to corresponding bin given separate offsets,
     * for multi-classification with full hessian.
     *
     * @param index bin index.
     * @param grad gradient values.
     * @param gradOffset offset for gradients.
     * @param hess hessian values.
     * @param hessOffset offset for hessians.
     */
    public void accumulate(int index, double[] grad, int gradOffset,
                           double[] hess, int hessOffset) {
        int gradBinOffset = index * numClass;
        int hessBinOffset = index * (numClass * (numClass + 1)) / 2;
        for (int i = 0; i < numClass; i++)
            gradients[gradBinOffset + i] += grad[gradOffset + i];
        for (int i = 0; i < numClass * (numClass + 1) / 2; i++)
            hessians[hessBinOffset + i] += hess[hessOffset + i];
    }

    /**
     * Accumulate a GradPair to corresponding bin,
     * for both binary- and multi-classification.
     *
     * @param index bin index.
     * @param gradPair gradient pair.
     */
    public void accumulate(int index, GradPair gradPair) {
        if (numClass == 2) {
            BinaryGradPair binary = (BinaryGradPair) gradPair;
            gradients[index] += binary.getGrad();
            hessians[index] += binary.getHess();
        } else if (!fullHessian) {
            MultiGradPair multi = (MultiGradPair) gradPair;
            double[] grad = multi.getGrad();
            double[] hess = multi.getHess();
            int offset = index * numClass;
            for (int i = 0; i < numClass; i++) {
                gradients[offset + i] += grad[i];
                hessians[offset + i] += hess[i];
            }
        } else {
            MultiGradPair multi = (MultiGradPair) gradPair;
            double[] grad = multi.getGrad();
            double[] hess = multi.getHess();
            int gradOffset = index * numClass;
            int hessOffset = index * ((numClass * (numClass + 1)) >> 1);
            for (int i = 0; i < grad.length; i++)
                gradients[gradOffset + i] += grad[i];
            for (int i = 0; i < hess.length; i++)
                hessians[hessOffset + i] += hess[i];
        }
    }

    /**
     * Plus two histograms. The original histogram is remain unchanged.
     *
     * @param other addend.
     * @return a new histogram after addition.
     */
    public Histogram plus(Histogram other) {
        Histogram res = new Histogram(numBin, numClass, fullHessian);
        if (numClass == 2 || !fullHessian) {
            for (int i = 0; i < this.gradients.length; i++) {
                res.gradients[i] = this.gradients[i] + other.gradients[i];
                res.hessians[i] = this.hessians[i] + other.hessians[i];
            }
        } else {
            for (int i = 0; i < this.gradients.length; i++)
                res.gradients[i] = this.gradients[i] + other.gradients[i];
            for (int i = 0; i < this.hessians.length; i++)
                res.hessians[i] = this.hessians[i] + other.hessians[i];
        }
        return res;
    }

    /**
     * Subtract two histograms. The original histogram is remain unchanged.
     *
     * @param other subtrahend
     * @return a new histogram after subtraction.
     */
    public Histogram subtract(Histogram other) {
        Histogram res = new Histogram(numBin, numClass, fullHessian);
        if (numClass == 2 || !fullHessian) {
            for (int i = 0; i < this.gradients.length; i++) {
                res.gradients[i] = this.gradients[i] - other.gradients[i];
                res.hessians[i] = this.hessians[i] - other.hessians[i];
            }
        } else {
            for (int i = 0; i < this.gradients.length; i++)
                res.gradients[i] = this.gradients[i] - other.gradients[i];
            for (int i = 0; i < this.hessians.length; i++)
                res.hessians[i] = this.hessians[i] - other.hessians[i];
        }
        return res;
    }

    /**
     * In place addition.
     *
     * @param other addend.
     */
    public void plusBy(Histogram other) {
        if (numClass == 2 || !fullHessian) {
            for (int i = 0; i < this.gradients.length; i++) {
                this.gradients[i] += other.gradients[i];
                this.hessians[i] += other.hessians[i];
            }
        } else {
            for (int i = 0; i < this.gradients.length; i++)
                this.gradients[i] += other.gradients[i];
            for (int i = 0; i < this.hessians.length; i++)
                this.hessians[i] += other.hessians[i];
        }
    }

    /**
     * In place subtraction.
     *
     * @param other subtrahend.
     */
    public void subtractBy(Histogram other) {
        if (numClass == 2 || !fullHessian) {
            for (int i = 0; i < this.gradients.length; i++) {
                this.gradients[i] -= other.gradients[i];
                this.hessians[i] -= other.hessians[i];
            }
        } else {
            for (int i = 0; i < this.gradients.length; i++)
                this.gradients[i] -= other.gradients[i];
            for (int i = 0; i < this.hessians.length; i++)
                this.hessians[i] -= other.hessians[i];
        }
    }

    /**
     * Get one histogram bin as a GradPair.
     *
     * @param index bin index.
     * @return GradPair.
     */
    public GradPair get(int index) {
        if (numClass == 2) {
            return new BinaryGradPair(gradients[index], hessians[index]);
        } else {
            double[] grad = Arrays.copyOfRange(gradients,
                    index * numClass, (index + 1) * numClass);
            int size = fullHessian ? ((numClass * (numClass + 1)) >> 1) : numClass;
            double[] hess = Arrays.copyOfRange(hessians,
                    index * size, (index + 1) * size);
            return new MultiGradPair(grad, hess);
        }
    }

    /**
     * Get one histogram bin to a GradPair.
     *
     * @param index bin index.
     * @param gp GradPair.
     */
    public void get(int index, GradPair gp) {
        if (numClass == 2) {
            ((BinaryGradPair) gp).set(gradients[index], hessians[index]);
        } else if (!fullHessian) {
            ((MultiGradPair) gp).set(gradients, hessians, index * numClass);
        } else {
            int gradOffset = index * numClass;
            int hessOffset = index * ((numClass * (numClass + 1)) >> 1);
            ((MultiGradPair) gp).set(gradients, gradOffset, hessians, hessOffset);
        }
    }

    /**
     * Add a histogram bin to a GradPair.
     *
     * @param gp GradPair.
     * @param index bin index.
     */
    public void plusTo(GradPair gp, int index) {
        if (numClass == 2) {
            ((BinaryGradPair) gp).plusBy(gradients[index], hessians[index]);
        } else if (!fullHessian) {
            MultiGradPair multi = (MultiGradPair) gp;
            double[] grad = multi.getGrad();
            double[] hess = multi.getHess();
            int offset = index * numClass;
            for (int i = 0; i < numClass; i++) {
                grad[i] += gradients[offset + i];
                hess[i] += hessians[offset + i];
            }
        } else {
            MultiGradPair multi = (MultiGradPair) gp;
            double[] grad = multi.getGrad();
            double[] hess = multi.getHess();
            int gradOffset = index * grad.length;
            int hessOffset = index * hess.length;
            for (int i = 0; i < grad.length; i++)
                grad[i] += gradients[gradOffset + i];
            for (int i = 0; i < hess.length; i++)
                hess[i] += hessians[hessOffset + i];
        }
    }

    /**
     * Subtract a GradPair from a histogram bin.
     *
     * @param gp GradPair.
     * @param index bin index.
     */
    public void subtractTo(GradPair gp, int index) {
        if (numClass == 2) {
            ((BinaryGradPair) gp).subtractBy(gradients[index], hessians[index]);
        } else if (!fullHessian) {
            MultiGradPair multi = (MultiGradPair) gp;
            double[] grad = multi.getGrad();
            double[] hess = multi.getHess();
            int offset = index * numClass;
            for (int i = 0; i < numClass; i++) {
                grad[i] -= gradients[offset + i];
                hess[i] -= hessians[offset + i];
            }
        } else {
            MultiGradPair multi = (MultiGradPair) gp;
            double[] grad = multi.getGrad();
            double[] hess = multi.getHess();
            int gradOffset = index * grad.length;
            int hessOffset = index * hess.length;
            for (int i = 0; i < grad.length; i++)
                grad[i] -= gradients[gradOffset + i];
            for (int i = 0; i < hess.length; i++)
                hess[i] -= hessians[hessOffset + i];
        }
    }

    /**
     * Helper function for histogram scanning.
     * For a histogram bin, add it to right grad pair,
     * and subtract left grad pair from it.
     *
     * @param index bin index.
     * @param left left grad pair.
     * @param right right grad pair.
     */
    public void scan(int index, GradPair left, GradPair right) {
        if (numClass == 2) {
            ((BinaryGradPair) left).plusBy(gradients[index], hessians[index]);
            ((BinaryGradPair) right).subtractBy(gradients[index], hessians[index]);
        } else if (!fullHessian) {
            MultiGradPair leftMulti = (MultiGradPair) left;
            double[] leftGrad = leftMulti.getGrad();
            double[] leftHess = leftMulti.getHess();
            MultiGradPair rightMulti = (MultiGradPair) right;
            double[] rightGrad = rightMulti.getGrad();
            double[] rightHess = rightMulti.getHess();
            int offset = index * numClass;
            for (int i = 0; i < numClass; i++) {
                leftGrad[i] += gradients[offset + i];
                leftHess[i] += hessians[offset + i];
                rightGrad[i] -= gradients[offset + i];
                rightHess[i] -= hessians[offset + i];
            }
        } else {
            MultiGradPair leftMulti = (MultiGradPair) left;
            double[] leftGrad = leftMulti.getGrad();
            double[] leftHess = leftMulti.getHess();
            MultiGradPair rightMulti = (MultiGradPair) right;
            double[] rightGrad = rightMulti.getGrad();
            double[] rightHess = rightMulti.getHess();
            int gradOffset = index * leftGrad.length;
            int hessOffset = index * leftHess.length;
            for (int i = 0; i < leftGrad.length; i++) {
                leftGrad[i] += gradients[gradOffset + i];
                rightGrad[i] -= gradients[gradOffset + i];
            }
            for (int i = 0; i < leftHess.length; i++) {
                leftHess[i] += hessians[hessOffset + i];
                rightHess[i] -= hessians[hessOffset + i];
            }
        }
    }

    /**
     * Sum of gradients and hessians in the histogram.
     *
     * @param start starting point.
     * @param end ending point.
     * @return GradPair.
     */
    public GradPair sum(int start, int end) {
        if (numClass == 2) {
            double sumGrad = 0.0;
            double sumHess = 0.0;
            for (int i = start; i < end; i++) {
                sumGrad += gradients[i];
                sumHess += hessians[i];
            }
            return new BinaryGradPair(sumGrad, sumHess);
        } else if (!fullHessian) {
            double[] sumGrad = new double[numClass];
            double[] sumHess = new double[numClass];
            for (int i = start * numClass; i < end * numClass; i += numClass) {
                for (int j = 0; j < numClass; j++) {
                    sumGrad[j] += gradients[i + j];
                    sumHess[j] += hessians[i + j];
                }
            }
            return new MultiGradPair(sumGrad, sumHess);
        } else {
            double[] sumGrad = new double[numClass];
            double[] sumHess = new double[numClass * (numClass + 1) / 2];
            for (int i = start; i < end; i++) {
                int gradOffset = i * sumGrad.length;
                for (int j = 0; j < sumGrad.length; j++)
                    sumGrad[j] += gradients[gradOffset + j];
                int hessOffset = i * sumHess.length;
                for (int j = 0; j < sumHess.length; j++)
                    sumHess[j] += hessians[hessOffset + j];
            }
            return new MultiGradPair(sumGrad, sumHess);
        }
    }

    public GradPair sum() {
        return sum(0, numBin);
    }

    /**
     * Set histogram to zero.
     *
     */
    public void clear() {
        Arrays.fill(gradients, 0.0);
        Arrays.fill(hessians, 0.0);
    }

    public int getNumBin() {
        return numBin;
    }

    public int getNumClass() {
        return numClass;
    }

    public boolean isFullHessian() {
        return fullHessian;
    }

    public double[] getGradients() {
        return gradients;
    }

    public double[] getHessians() {
        return hessians;
    }
}
