package com.tencent.angel.sona.ml.regression

import com.tencent.angel.sona.ml.util.AngelTestUtils


class RobustReg extends AngelTestUtils {
  test("robustreg_train") {
    val trainData = libsvm.load("./data/angel/abalone/abalone_8d_train.libsvm")

    val regressor = new AngelRegressor()
      .setModelJsonFile("./angelml/src/test/jsons/robustreg.json")
      .setModelSize(10)
      .setNumBatch(10)
      .setMaxIter(2)
      .setLearningRate(0.1)


    val model = regressor.fit(trainData)

    model.write.overwrite().save("trained_models/robustreg")
  }


//  test("robust_predict") {
//    val trainData = libsvm.load("./data/angel/abalone/abalone_8d_train.libsvm")
//
//    val predictor = AngelRegressorModel.read.load("trained_models/robustreg")
//
//    val res = predictor.transform(trainData)
//    res.show()
//    res.write.mode("overwrite").save("predict_results/robustreg")
//  }
}
