package org.apache.spark.angel.psf.gcn;

import com.tencent.angel.ml.math2.vector.LongFloatVector;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;

public class GetLabelsResult extends GetResult {
  private LongFloatVector vector;
  public GetLabelsResult(LongFloatVector vector) {
    this.vector = vector;
  }

  public LongFloatVector getVector() {
    return vector;
  }
}
