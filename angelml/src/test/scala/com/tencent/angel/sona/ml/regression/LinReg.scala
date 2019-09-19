package com.tencent.angel.sona.ml.regression

import com.tencent.angel.sona.ml.util.AngelTestUtils


class LinReg extends AngelTestUtils {

  test("linreg_train") {
    val trainData = libsvm.load("./data/angel/abalone/abalone_8d_train.libsvm")

    val classifier = new AngelRegressor()
      .setModelJsonFile("./angelml/src/test/jsons/linreg.json")
      .setNumBatch(10)
      .setMaxIter(2)
      .setLearningRate(0.1)

    val model = classifier.fit(trainData)

    model.write.overwrite().save("trained_models/linreg")
  }
}
