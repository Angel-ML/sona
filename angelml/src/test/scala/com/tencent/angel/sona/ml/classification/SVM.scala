package com.tencent.angel.sona.ml.classification

import com.tencent.angel.sona.ml.util.AngelTestUtils

class SVM extends AngelTestUtils {

  test("svm_train") {
    val trainData = dummy.load("./data/angel/a9a/a9a_123d_train.dummy")

    val svm_classifier = new AngelClassifier()
      .setModelJsonFile("./angelml/src/test/jsons/svm.json")
      .setNumBatch(10)
      .setMaxIter(2)
      .setLearningRate(0.1)
      .setNumField(14)

    val svm_model = svm_classifier.fit(trainData)

    svm_model.write.overwrite().save("trained_models/svm")
  }

//  test("svm_predict") {
//    val trainData = dummy.load("./data/angel/a9a/a9a_123d_train.dummy")
//
//    val predictor = AngelClassifierModel.read.load("trained_models/svm")
//
//    val res = predictor.transform(trainData)
//    res.show()
//    res.write.mode("overwrite").save("predict_results/svm")
//  }

}
