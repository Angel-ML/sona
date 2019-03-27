package com.tencent.angel.sona


import com.tencent.angel.ml.math2.utils.RowType
import com.tencent.angel.sona.classifier.{AngelClassifier, AngelClassifierModel}
import com.tencent.angel.sona.common.measure.ClassificationSummary._
import com.tencent.angel.sona.core.DriverContext
import com.tencent.angel.sona.utils.PlotUtils
import org.apache.spark.sql.{DataFrame, SPKSQLUtils}

class ClassificationTest extends TestBase {

  test("train_lr") {
    DriverContext.get(spark.sparkContext.getConf).startAngel()
    val train: DataFrame = libsvmLoader.load("data/angel/a9a/a9a_123d_train.libsvm")
    val testData: DataFrame = SPKSQLUtils.readDummy(spark, "data/angel/a9a/a9a_123d_test.dummy", 123)

    train.printSchema()
    testData.printSchema()

    // DriverContext.get().getConf.setBoolean("angel.output.path.deleteonexist", true)

    val estimator = new AngelClassifier()
      .setModelJsonFile("src/test/jsons/logreg.json")
      .setMaxIter(20)
      .setLearningRate(0.5)
      .setModelType(RowType.T_DOUBLE_DENSE.toString)
      .setModelName("lr")
      .setFeaturesCol("features")
      .setLabelCol("label")

    val model = estimator.fit(train)

    PlotUtils.plotTrainStat(model.summary, "src/test/pngs/lr_train.png")

    val summary = model.evaluate(testData)

    PlotUtils.plotSummary(summary, "src/test/pngs/evaluate.png")

    estimator.save(check("src/test/models/lr_estimator"))
    model.save(check("src/test/models/lr"))
    DriverContext.get().stopAngel()
  }

  test("predict_lr") {
    DriverContext.get(spark.sparkContext.getConf).startAngel()
    val train: DataFrame = libsvmLoader.load("data/angel/a9a/a9a_123d_train.libsvm")
    val testData: DataFrame = SPKSQLUtils.readDummy(spark, "data/angel/a9a/a9a_123d_test.dummy", 123)

    train.printSchema()
    testData.printSchema()

    val model = AngelClassifierModel.load("src/test/models/lr")
      .setPredictionCol("opCol")
      .setProbabilityCol("proba")

    val res = model.transform(testData)

    res.printSchema()
    res.rdd.saveAsTextFile(check("src/test/results/result_lr"))

    DriverContext.get().stopAngel()
  }

  test("IncTrain_lr") {
    DriverContext.get(spark.sparkContext.getConf).startAngel()
    val train: DataFrame = libsvmLoader.load("data/angel/a9a/a9a_123d_train.libsvm")
    val testData: DataFrame = SPKSQLUtils.readDummy(spark, "data/angel/a9a/a9a_123d_test.dummy", 123)

    train.printSchema()
    testData.printSchema()

    val estimator = AngelClassifier.load("src/test/models/lr_estimator")
      .setInitModelPath("src/test/models/lr")

    val model = estimator.fit(testData)

    model.save(check("src/test/models/lr2"))
    DriverContext.get().stopAngel()
  }

  test("train_softmax") {
    DriverContext.get(spark.sparkContext.getConf).startAngel()
    val train: DataFrame = libsvmLoader.load("data/angel/protein/protein_357d_train.libsvm")
    val testData: DataFrame = libsvmLoader.load("data/angel/protein/protein_357d_test.libsvm")

    train.printSchema()
    testData.printSchema()

    val estimator = new AngelClassifier()
      .setModelJsonFile("src/test/jsons/softmax.json")
      .setMaxIter(50)
      .setLearningRate(0.01)
      .setModelType(RowType.T_DOUBLE_DENSE.toString)
      .setModelName("softmax")
      .setFeaturesCol("features")
      .setLabelCol("label")

    val model = estimator.fit(train)

    PlotUtils.plotTrainStat(model.summary, "src/test/pngs/softmax_train.png")

    val summary = model.evaluate(testData)

    PlotUtils.plotSummary(summary, "src/test/pngs/softmax_evaluate.png")
    estimator.save(check("src/test/models/softmax_estimator"))
    model.save(check("src/test/models/softmax"))
    DriverContext.get().stopAngel()
  }

  test("predict_softmax") {
    DriverContext.get(spark.sparkContext.getConf).startAngel()
    val testData: DataFrame = libsvmLoader.load("data/angel/protein/protein_357d_test.libsvm")

    testData.printSchema()

    val model = AngelClassifierModel.load("src/test/models/softmax")
      .setPredictionCol("opCol")
      .setProbabilityCol("proba")

    val res = model.transform(testData)

    res.printSchema()
    res.rdd.saveAsTextFile(check("src/test/results/softmax_result"))

    DriverContext.get().stopAngel()
  }

  test("IncTrain_softmax") {
    DriverContext.get(spark.sparkContext.getConf).startAngel()
    val testData: DataFrame = libsvmLoader.load("data/angel/protein/protein_357d_test.libsvm")

    testData.printSchema()

    val estimator = AngelClassifier.load("src/test/models/softmax_estimator")
      .setInitModelPath("src/test/models/softmax")

    val model = estimator.fit(testData)

    model.save(check("src/test/models/softmax2"))
    DriverContext.get().stopAngel()
  }

}
