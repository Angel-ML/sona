package com.tencent.angel.sona

import com.tencent.angel.ml.math2.utils.RowType
import com.tencent.angel.sona.core.DriverContext
import com.tencent.angel.sona.regresser.{AngelRegressor, AngelRegressorModel}
import com.tencent.angel.sona.utils.PlotUtils
import org.apache.spark.sql.DataFrame

class RegressionTest extends TestBase {
  val train: DataFrame = libsvmLoader.load("data/angel/mg/mg_6d_train.libsvm").repartition(4)

  test("train_linearReg") {
    DriverContext.get(spark.sparkContext.getConf).startAngel()

    val estimator = new AngelRegressor()
      .setModelJsonFile("src/test/jsons/linreg.json")
      .setMaxIter(20)
      .setLearningRate(0.1)
      .setModelType(RowType.T_DOUBLE_DENSE.toString)
      .setModelName("linear")
      .setFeaturesCol("features")
      .setLabelCol("label")

    val model = estimator.fit(train)

    PlotUtils.plotTrainStat(model.summary, "src/test/pngs/linear.png")

    val summary = model.evaluate(train)

    PlotUtils.plotSummary(summary, "src/test/pngs/linear_evaluate.png")

    estimator.save(check("src/test/models/linear_estimator"))
    model.save(check("src/test/models/linear"))
    DriverContext.get().stopAngel()
  }

  test("predict_linearReg") {
    DriverContext.get(spark.sparkContext.getConf).startAngel()

    val model = AngelRegressorModel.load("src/test/models/linear")

    val res = model.transform(train)

    res.printSchema()
    res.rdd.saveAsTextFile(check("src/test/results/result_linearReg"))
    DriverContext.get().stopAngel()
  }

  test("incTrain_linearReg") {
    DriverContext.get(spark.sparkContext.getConf).startAngel()

    val estimator = AngelRegressor.load("src/test/models/linear_estimator")
      .setInitModelPath("src/test/models/linear")

    val model = estimator.fit(train)
    model.save(check("src/test/models/linear2"))

    DriverContext.get().stopAngel()
  }

}
