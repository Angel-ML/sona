package com.tencent.angel.sona.ml.classification

import com.tencent.angel.sona.ml.util.AngelTestUtils

class LogReg extends AngelTestUtils {

  test("logreg_train") {
    val trainData = libsvm.load("data/angel/a9a/a9a_123d_train.libsvm")

    val classifier = new AngelClassifier()
      .setModelJsonFile("./angelml/src/test/jsons/logreg.json")
      .setNumClass(2)
      .setNumBatch(10)
      .setMaxIter(2)
      .setLearningRate(0.1)
      .setNumField(14)

    val model = classifier.fit(trainData)

    model.write.overwrite().save("trained_models/logreg")
  }

}
