package com.tencent.angel.sona.ml.classification

import com.tencent.angel.sona.ml.util.AngelTestUtils

class DAW extends AngelTestUtils {
  test("daw_train") {
    val trainData = libsvm.load("./data/angel/census/census_148d_train.libsvm")

    val classifier = new AngelClassifier()
      .setModelJsonFile("./angelml/src/test/jsons/daw.json")
      .setNumClass(2)
      .setNumBatch(10)
      .setMaxIter(2)
      .setLearningRate(0.1)
      .setNumField(13)

    val model = classifier.fit(trainData)

    model.write.overwrite().save("trained_models/daw")

    classifier.releaseAngelModel()
  }

//  test("daw_predict") {
//    val trainData = libsvm.load("./data/angel/census/census_148d_train.libsvm")
//
//    val predictor = AngelClassifierModel.read.load("trained_models/daw")
//
//    val res = predictor.transform(trainData)
//    res.show()
//    res.write.mode("overwrite").save("predict_results/daw")
//  }
}
