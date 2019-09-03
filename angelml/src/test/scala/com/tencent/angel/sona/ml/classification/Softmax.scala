package com.tencent.angel.sona.ml.classification

import com.tencent.angel.sona.ml.util.AngelTestUtils

class Softmax extends AngelTestUtils {

  test("softmax_train") {
    val trainData = libsvm.load("./data/angel/protein/protein_357d_train.libsvm")

    val classifier = new AngelClassifier()
      .setModelJsonFile("./angelml/src/test/jsons/softmax.json")
      .setNumClass(3)
      .setNumBatch(10)
      .setMaxIter(2)
      .setLearningRate(0.1)
      .setNumField(22)

    val model = classifier.fit(trainData)

    model.write.overwrite().save("trained_models/softmax")
  }

}
