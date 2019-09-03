package com.tencent.angel.sona.ml.classification

import com.tencent.angel.sona.ml.util.AngelTestUtils

class NFM extends AngelTestUtils {

  test("nfm_train") {
    val trainData = dummy.load("data/angel/census/census_148d_train.dummy")

    val classifier = new AngelClassifier()
      .setModelJsonFile("./angelml/src/test/jsons/nfm.json")
      .setNumClass(2)
      .setNumBatch(10)
      .setMaxIter(2)
      .setLearningRate(0.1)
      .setNumField(13)

    val model = classifier.fit(trainData)

    model.write.overwrite().save("trained_models/nfm")
  }

}
