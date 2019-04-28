/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.angelml.tuning

import java.io.File

import org.apache.spark.SparkFunSuite
import org.apache.spark.angelml.{Estimator, Model, Pipeline}
import org.apache.spark.angelml.classification.{LogisticRegression, LogisticRegressionModel, OneVsRest}
import org.apache.spark.angelml.classification.LogisticRegressionSuite.generateLogisticInput
import org.apache.spark.angelml.evaluation.{BinaryClassificationEvaluator, Evaluator, MulticlassClassificationEvaluator, RegressionEvaluator}
import org.apache.spark.angelml.feature.HashingTF
import org.apache.spark.angelml.linalg.{Vector, Vectors}
import org.apache.spark.angelml.param.ParamMap
import org.apache.spark.angelml.param.shared.HasInputCol
import org.apache.spark.angelml.regression.LinearRegression
import org.apache.spark.angelml.util.{DefaultReadWriteTest, MLTest, MLTestingUtils}
import org.apache.spark.angelml.util.LinearDataGenerator
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types.StructType

class CrossValidatorSuite
  extends MLTest with DefaultReadWriteTest {

  import testImplicits._

  @transient var dataset: Dataset[_] = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    dataset = sc.parallelize(generateLogisticInput(1.0, 1.0, 100, 42), 2).toDF()
  }

  test("cross validation with logistic regression") {
    val lr = new LogisticRegression
    val lrParamMaps = new ParamGridBuilder()
      .addGrid(lr.regParam, Array(0.001, 1000.0))
      .addGrid(lr.maxIter, Array(0, 10))
      .build()
    val eval = new BinaryClassificationEvaluator
    val cv = new CrossValidator()
      .setEstimator(lr)
      .setEstimatorParamMaps(lrParamMaps)
      .setEvaluator(eval)
      .setNumFolds(3)
    val cvModel = cv.fit(dataset)

    MLTestingUtils.checkCopyAndUids(cv, cvModel)

    val parent = cvModel.bestModel.parent.asInstanceOf[LogisticRegression]
    assert(parent.getRegParam === 0.001)
    assert(parent.getMaxIter === 10)
    assert(cvModel.avgMetrics.length === lrParamMaps.length)

    val result = cvModel.transform(dataset).select("prediction").as[Double].collect()
    testTransformerByGlobalCheckFunc[(Double, Vector)](dataset.toDF(), cvModel, "prediction") {
      rows =>
        val result2 = rows.map(_.getDouble(0))
        assert(result === result2)
    }
  }

  test("cross validation with linear regression") {
    val dataset = sc.parallelize(
      LinearDataGenerator.generateLinearInput(
        6.3, Array(4.7, 7.2), Array(0.9, -1.3), Array(0.7, 1.2), 100, 42, 0.1), 2).toDF()

    val trainer = new LinearRegression().setSolver("l-bfgs")
    val lrParamMaps = new ParamGridBuilder()
      .addGrid(trainer.regParam, Array(1000.0, 0.001))
      .addGrid(trainer.maxIter, Array(0, 10))
      .build()
    val eval = new RegressionEvaluator()
    val cv = new CrossValidator()
      .setEstimator(trainer)
      .setEstimatorParamMaps(lrParamMaps)
      .setEvaluator(eval)
      .setNumFolds(3)
    val cvModel = cv.fit(dataset)
    val parent = cvModel.bestModel.parent.asInstanceOf[LinearRegression]
    assert(parent.getRegParam === 0.001)
    assert(parent.getMaxIter === 10)
    assert(cvModel.avgMetrics.length === lrParamMaps.length)

    eval.setMetricName("r2")
    val cvModel2 = cv.fit(dataset)
    val parent2 = cvModel2.bestModel.parent.asInstanceOf[LinearRegression]
    assert(parent2.getRegParam === 0.001)
    assert(parent2.getMaxIter === 10)
    assert(cvModel2.avgMetrics.length === lrParamMaps.length)
  }

  test("transformSchema should check estimatorParamMaps") {
    import CrossValidatorSuite.{MyEstimator, MyEvaluator}

    val est = new MyEstimator("est")
    val eval = new MyEvaluator
    val paramMaps = new ParamGridBuilder()
      .addGrid(est.inputCol, Array("input1", "input2"))
      .build()

    val cv = new CrossValidator()
      .setEstimator(est)
      .setEstimatorParamMaps(paramMaps)
      .setEvaluator(eval)

    cv.transformSchema(new StructType()) // This should pass.

    val invalidParamMaps = paramMaps :+ ParamMap(est.inputCol -> "")
    cv.setEstimatorParamMaps(invalidParamMaps)
    intercept[IllegalArgumentException] {
      cv.transformSchema(new StructType())
    }
  }

  test("cross validation with parallel evaluation") {
    val lr = new LogisticRegression
    val lrParamMaps = new ParamGridBuilder()
      .addGrid(lr.regParam, Array(0.001, 1000.0))
      .addGrid(lr.maxIter, Array(0, 3))
      .build()
    val eval = new BinaryClassificationEvaluator
    val cv = new CrossValidator()
      .setEstimator(lr)
      .setEstimatorParamMaps(lrParamMaps)
      .setEvaluator(eval)
      .setNumFolds(2)
      .setParallelism(1)
    val cvSerialModel = cv.fit(dataset)
    cv.setParallelism(2)
    val cvParallelModel = cv.fit(dataset)

    val serialMetrics = cvSerialModel.avgMetrics
    val parallelMetrics = cvParallelModel.avgMetrics
    assert(serialMetrics === parallelMetrics)

    val parentSerial = cvSerialModel.bestModel.parent.asInstanceOf[LogisticRegression]
    val parentParallel = cvParallelModel.bestModel.parent.asInstanceOf[LogisticRegression]
    assert(parentSerial.getRegParam === parentParallel.getRegParam)
    assert(parentSerial.getMaxIter === parentParallel.getMaxIter)
  }

  test("read/write: CrossValidator with simple estimator") {
    val lr = new LogisticRegression().setMaxIter(3)
    val evaluator = new BinaryClassificationEvaluator()
      .setMetricName("areaUnderPR")  // not default metric
    val paramMaps = new ParamGridBuilder()
        .addGrid(lr.regParam, Array(0.1, 0.2))
        .build()
    val cv = new CrossValidator()
      .setEstimator(lr)
      .setEvaluator(evaluator)
      .setNumFolds(20)
      .setEstimatorParamMaps(paramMaps)
      .setSeed(42L)
      .setParallelism(2)
      .setCollectSubModels(true)

    val cv2 = testDefaultReadWrite(cv, testParams = false)

    assert(cv.uid === cv2.uid)
    assert(cv.getNumFolds === cv2.getNumFolds)
    assert(cv.getSeed === cv2.getSeed)
    assert(cv.getParallelism === cv2.getParallelism)
    assert(cv.getCollectSubModels === cv2.getCollectSubModels)

    assert(cv2.getEvaluator.isInstanceOf[BinaryClassificationEvaluator])
    val evaluator2 = cv2.getEvaluator.asInstanceOf[BinaryClassificationEvaluator]
    assert(evaluator.uid === evaluator2.uid)
    assert(evaluator.getMetricName === evaluator2.getMetricName)

    cv2.getEstimator match {
      case lr2: LogisticRegression =>
        assert(lr.uid === lr2.uid)
        assert(lr.getMaxIter === lr2.getMaxIter)
      case other =>
        throw new AssertionError(s"Loaded CrossValidator expected estimator of type" +
          s" LogisticRegression but found ${other.getClass.getName}")
    }

    ValidatorParamsSuiteHelpers
      .compareParamMaps(cv.getEstimatorParamMaps, cv2.getEstimatorParamMaps)
  }

  test("CrossValidator expose sub models") {
    val lr = new LogisticRegression
    val lrParamMaps = new ParamGridBuilder()
      .addGrid(lr.regParam, Array(0.001, 1000.0))
      .addGrid(lr.maxIter, Array(0, 3))
      .build()
    val eval = new BinaryClassificationEvaluator
    val numFolds = 3
    val subPath = new File(tempDir, "testCrossValidatorSubModels")

    val cv = new CrossValidator()
      .setEstimator(lr)
      .setEstimatorParamMaps(lrParamMaps)
      .setEvaluator(eval)
      .setNumFolds(numFolds)
      .setParallelism(1)
      .setCollectSubModels(true)

    val cvModel = cv.fit(dataset)

    assert(cvModel.hasSubModels && cvModel.subModels.length == numFolds)
    cvModel.subModels.foreach(array => assert(array.length == lrParamMaps.length))

    // Test the default value for option "persistSubModel" to be "true"
    val savingPathWithSubModels = new File(subPath, "cvModel3").getPath
    cvModel.save(savingPathWithSubModels)
    val cvModel3 = CrossValidatorModel.load(savingPathWithSubModels)
    assert(cvModel3.hasSubModels && cvModel3.subModels.length == numFolds)
    cvModel3.subModels.foreach(array => assert(array.length == lrParamMaps.length))

    val savingPathWithoutSubModels = new File(subPath, "cvModel2").getPath
    cvModel.write.option("persistSubModels", "false").save(savingPathWithoutSubModels)
    val cvModel2 = CrossValidatorModel.load(savingPathWithoutSubModels)
    assert(!cvModel2.hasSubModels)

    for (i <- 0 until numFolds) {
      for (j <- 0 until lrParamMaps.length) {
        assert(cvModel.subModels(i)(j).asInstanceOf[LogisticRegressionModel].uid ===
          cvModel3.subModels(i)(j).asInstanceOf[LogisticRegressionModel].uid)
      }
    }

    val savingPathTestingIllegalParam = new File(subPath, "cvModel4").getPath
    intercept[IllegalArgumentException] {
      cvModel2.write.option("persistSubModels", "true").save(savingPathTestingIllegalParam)
    }
  }

  test("read/write: Persistence of nested estimator works if parent directory changes") {
    val ova = new OneVsRest().setClassifier(new LogisticRegression)
    val evaluator = new MulticlassClassificationEvaluator()
      .setMetricName("accuracy")
    val classifier1 = new LogisticRegression().setRegParam(2.0)
    val classifier2 = new LogisticRegression().setRegParam(3.0)
    // params that are not JSON serializable must inherit from Params
    val paramMaps = new ParamGridBuilder()
      .addGrid(ova.classifier, Seq(classifier1, classifier2))
      .build()
    val cv = new CrossValidator()
      .setEstimator(ova)
      .setEvaluator(evaluator)
      .setNumFolds(20)
      .setEstimatorParamMaps(paramMaps)

    ValidatorParamsSuiteHelpers.testFileMove(cv, tempDir)
  }

  test("read/write: CrossValidator fails for extraneous Param") {
    val lr = new LogisticRegression()
    val lr2 = new LogisticRegression()
    val evaluator = new BinaryClassificationEvaluator()
    val paramMaps = new ParamGridBuilder()
      .addGrid(lr.regParam, Array(0.1, 0.2))
      .addGrid(lr2.regParam, Array(0.1, 0.2))
      .build()
    val cv = new CrossValidator()
      .setEstimator(lr)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramMaps)
    withClue("CrossValidator.write failed to catch extraneous Param error") {
      intercept[IllegalArgumentException] {
        cv.write
      }
    }
  }

  test("read/write: CrossValidatorModel") {
    val lr = new LogisticRegression()
      .setThreshold(0.6)
    val lrModel = new LogisticRegressionModel(lr.uid, Vectors.dense(1.0, 2.0), 1.2)
      .setThreshold(0.6)
    val evaluator = new BinaryClassificationEvaluator()
      .setMetricName("areaUnderPR")  // not default metric
    val paramMaps = new ParamGridBuilder()
        .addGrid(lr.regParam, Array(0.1, 0.2))
        .build()
    val cv = new CrossValidatorModel("cvUid", lrModel, Array(0.3, 0.6))
    cv.set(cv.estimator, lr)
      .set(cv.evaluator, evaluator)
      .set(cv.numFolds, 20)
      .set(cv.estimatorParamMaps, paramMaps)

    val cv2 = testDefaultReadWrite(cv, testParams = false)

    assert(cv.uid === cv2.uid)
    assert(cv.getNumFolds === cv2.getNumFolds)
    assert(cv.getSeed === cv2.getSeed)

    assert(cv2.getEvaluator.isInstanceOf[BinaryClassificationEvaluator])
    val evaluator2 = cv2.getEvaluator.asInstanceOf[BinaryClassificationEvaluator]
    assert(evaluator.uid === evaluator2.uid)
    assert(evaluator.getMetricName === evaluator2.getMetricName)

    cv2.getEstimator match {
      case lr2: LogisticRegression =>
        assert(lr.uid === lr2.uid)
        assert(lr.getThreshold === lr2.getThreshold)
      case other =>
        throw new AssertionError(s"Loaded CrossValidator expected estimator of type" +
          s" LogisticRegression but found ${other.getClass.getName}")
    }

   ValidatorParamsSuiteHelpers
     .compareParamMaps(cv.getEstimatorParamMaps, cv2.getEstimatorParamMaps)

    cv2.bestModel match {
      case lrModel2: LogisticRegressionModel =>
        assert(lrModel.uid === lrModel2.uid)
        assert(lrModel.getThreshold === lrModel2.getThreshold)
        assert(lrModel.coefficients === lrModel2.coefficients)
        assert(lrModel.intercept === lrModel2.intercept)
      case other =>
        throw new AssertionError(s"Loaded CrossValidator expected bestModel of type" +
          s" LogisticRegressionModel but found ${other.getClass.getName}")
    }
    assert(cv.avgMetrics === cv2.avgMetrics)
  }
}

object CrossValidatorSuite extends SparkFunSuite {

  abstract class MyModel extends Model[MyModel]

  class MyEstimator(override val uid: String) extends Estimator[MyModel] with HasInputCol {

    override def fit(dataset: Dataset[_]): MyModel = {
      throw new UnsupportedOperationException
    }

    override def transformSchema(schema: StructType): StructType = {
      require($(inputCol).nonEmpty)
      schema
    }

    override def copy(extra: ParamMap): MyEstimator = defaultCopy(extra)
  }

  class MyEvaluator extends Evaluator {

    override def evaluate(dataset: Dataset[_]): Double = {
      throw new UnsupportedOperationException
    }

    override def isLargerBetter: Boolean = true

    override val uid: String = "eval"

    override def copy(extra: ParamMap): MyEvaluator = defaultCopy(extra)
  }
}
