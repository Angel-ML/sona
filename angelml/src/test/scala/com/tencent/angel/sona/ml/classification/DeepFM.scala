package com.tencent.angel.sona.ml.classification

import com.tencent.angel.sona.ml.util.AngelTestUtils

class DeepFM extends AngelTestUtils {
  test("deepfm_train") {
    val trainData = dummy.load("data/angel/census/census_148d_train.dummy")

    val deepfm_classifier = new AngelClassifier()
      .setModelJsonFile("./angelml/src/test/jsons/deepfm.json")
      .setNumClass(2)
      .setNumBatch(10)
      .setMaxIter(2)
      .setLearningRate(0.1)
      .setNumField(13)

    val model = deepfm_classifier.fit(trainData)

    model.write.overwrite().save("trained_models/deepfm")
  }
}
