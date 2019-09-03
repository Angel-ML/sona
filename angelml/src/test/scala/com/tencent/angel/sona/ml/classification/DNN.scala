package com.tencent.angel.sona.ml.classification

import com.tencent.angel.sona.ml.feature.LabeledPoint
import com.tencent.angel.sona.ml.util.AngelTestUtils
import org.apache.spark.linalg.Vectors

class DNN extends AngelTestUtils {

  test("dnn_train") {
    val Data = spark.sparkContext.textFile("./data/angel/census/census_148d_train.dense").map(_.split(" "))
    val Data_ = Data.map(x => LabeledPoint(x(0).toDouble, Vectors.dense(x.slice(1, x.length-1).map(_.toDouble))))
    val trainData = spark.createDataFrame(Data_)

    trainData.show()

    val dnn_classifier = new AngelClassifier()
      .setModelJsonFile("./angelml/src/test/jsons/dnn.json")
      .setNumClass(2)
      .setNumBatch(10)
      .setMaxIter(2)
      .setLearningRate(0.1)
      .setNumField(13)

    println(dnn_classifier.getNumClass)
    val model = dnn_classifier.fit(trainData)

    model.write.overwrite().save("trained_models/dnn")
  }

}
