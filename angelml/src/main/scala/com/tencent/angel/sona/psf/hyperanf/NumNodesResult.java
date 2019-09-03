package com.tencent.angel.sona.psf.hyperanf;

import com.tencent.angel.ml.matrix.psf.get.base.GetResult;

public class NumNodesResult extends GetResult {
  private long result;

  public NumNodesResult(long result) {
    this.result = result;
  }

  public long getResult() {
    return result;
  }
}
