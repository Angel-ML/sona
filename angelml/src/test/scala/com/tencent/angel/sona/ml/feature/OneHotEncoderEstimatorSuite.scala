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

package com.tencent.angel.sona.ml.feature

import com.tencent.angel.sona.ml.attribute.{AttributeGroup, BinaryAttribute, NominalAttribute}
import org.apache.spark.linalg
import org.apache.spark.linalg.{VectorUDT, Vectors}
import com.tencent.angel.sona.ml.util.{DefaultReadWriteTest, MLTest}
import org.apache.spark.sql.{Encoder, Row}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._

class OneHotEncoderEstimatorSuite extends MLTest with DefaultReadWriteTest {

  import testImplicits._

  test("OneHotEncoderEstimator dropLast = false") {
    val data = Seq(
      Row(0.0, Vectors.sparse(3, Seq((0, 1.0)))),
      Row(1.0, Vectors.sparse(3, Seq((1, 1.0)))),
      Row(2.0, Vectors.sparse(3, Seq((2, 1.0)))),
      Row(0.0, Vectors.sparse(3, Seq((0, 1.0)))),
      Row(0.0, Vectors.sparse(3, Seq((0, 1.0)))),
      Row(2.0, Vectors.sparse(3, Seq((2, 1.0)))))

    val schema = StructType(Array(
        StructField("input", DoubleType),
        StructField("expected", new VectorUDT)))

    val df = spark.createDataFrame(sc.parallelize(data), schema)

    val encoder = new OneHotEncoderEstimator()
      .setInputCols(Array("input"))
      .setOutputCols(Array("output"))
    assert(encoder.getDropLast === true)
    encoder.setDropLast(false)
    assert(encoder.getDropLast === false)
    val model = encoder.fit(df)
    testTransformer[(Double, linalg.Vector)](df, model, "output", "expected") {
      case Row(output: linalg.Vector, expected: linalg.Vector) =>
        assert(output === expected)
    }
  }

  test("OneHotEncoderEstimator dropLast = true") {
    val data = Seq(
      Row(0.0, Vectors.sparse(2, Seq((0, 1.0)))),
      Row(1.0, Vectors.sparse(2, Seq((1, 1.0)))),
      Row(2.0, Vectors.sparse(2, Seq[(Int, Double)]())),
      Row(0.0, Vectors.sparse(2, Seq((0, 1.0)))),
      Row(0.0, Vectors.sparse(2, Seq((0, 1.0)))),
      Row(2.0, Vectors.sparse(2, Seq[(Int, Double)]())))

    val schema = StructType(Array(
        StructField("input", DoubleType),
        StructField("expected", new VectorUDT)))

    val df = spark.createDataFrame(sc.parallelize(data), schema)

    val encoder = new OneHotEncoderEstimator()
      .setInputCols(Array("input"))
      .setOutputCols(Array("output"))

    val model = encoder.fit(df)
    testTransformer[(Double, linalg.Vector)](df, model, "output", "expected") {
      case Row(output: linalg.Vector, expected: linalg.Vector) =>
        assert(output === expected)
    }
  }

  test("input column with ML attribute") {
    val attr = NominalAttribute.defaultAttr.withValues("small", "medium", "large")
    val df = Seq(0.0, 1.0, 2.0, 1.0).map(Tuple1.apply).toDF("size")
      .select(col("size").as("size", attr.toMetadata()))
    val encoder = new OneHotEncoderEstimator()
      .setInputCols(Array("size"))
      .setOutputCols(Array("encoded"))
    val model = encoder.fit(df)
    testTransformerByGlobalCheckFunc[(Double)](df, model, "encoded") { rows =>
        val group = AttributeGroup.fromStructField(rows.head.schema("encoded"))
        assert(group.size === 2)
        assert(group.getAttr(0) === BinaryAttribute.defaultAttr.withName("small").withIndex(0))
        assert(group.getAttr(1) === BinaryAttribute.defaultAttr.withName("medium").withIndex(1))
    }
  }

  test("input column without ML attribute") {
    val df = Seq(0.0, 1.0, 2.0, 1.0).map(Tuple1.apply).toDF("index")
    val encoder = new OneHotEncoderEstimator()
      .setInputCols(Array("index"))
      .setOutputCols(Array("encoded"))
    val model = encoder.fit(df)
    testTransformerByGlobalCheckFunc[(Double)](df, model, "encoded") { rows =>
      val group = AttributeGroup.fromStructField(rows.head.schema("encoded"))
      assert(group.size === 2)
      assert(group.getAttr(0) === BinaryAttribute.defaultAttr.withName("0").withIndex(0))
      assert(group.getAttr(1) === BinaryAttribute.defaultAttr.withName("1").withIndex(1))
    }
  }

  test("read/write") {
    val encoder = new OneHotEncoderEstimator()
      .setInputCols(Array("index"))
      .setOutputCols(Array("encoded"))
    testDefaultReadWrite(encoder)
  }

  test("OneHotEncoderModel read/write") {
    val instance = new OneHotEncoderModel("myOneHotEncoderModel", Array(1, 2, 3))
    val newInstance = testDefaultReadWrite(instance)
    assert(newInstance.categorySizes === instance.categorySizes)
  }

  test("OneHotEncoderEstimator with varying types") {
    val data = Seq(
      Row(0.0, Vectors.sparse(3, Seq((0, 1.0)))),
      Row(1.0, Vectors.sparse(3, Seq((1, 1.0)))),
      Row(2.0, Vectors.sparse(3, Seq((2, 1.0)))),
      Row(0.0, Vectors.sparse(3, Seq((0, 1.0)))),
      Row(0.0, Vectors.sparse(3, Seq((0, 1.0)))),
      Row(2.0, Vectors.sparse(3, Seq((2, 1.0)))))

    val schema = StructType(Array(
        StructField("input", DoubleType),
        StructField("expected", new VectorUDT)))

    val df = spark.createDataFrame(sc.parallelize(data), schema)

    class NumericTypeWithEncoder[A](val numericType: NumericType)
      (implicit val encoder: Encoder[(A, linalg.Vector)])

    val types = Seq(
      new NumericTypeWithEncoder[Short](ShortType),
      new NumericTypeWithEncoder[Long](LongType),
      new NumericTypeWithEncoder[Int](IntegerType),
      new NumericTypeWithEncoder[Float](FloatType),
      new NumericTypeWithEncoder[Byte](ByteType),
      new NumericTypeWithEncoder[Double](DoubleType),
      new NumericTypeWithEncoder[Decimal](DecimalType(10, 0))(ExpressionEncoder()))

    for (t <- types) {
      val dfWithTypes = df.select(col("input").cast(t.numericType), col("expected"))
      val estimator = new OneHotEncoderEstimator()
        .setInputCols(Array("input"))
        .setOutputCols(Array("output"))
        .setDropLast(false)

      val model = estimator.fit(dfWithTypes)
      testTransformer(dfWithTypes, model, "output", "expected") {
        case Row(output: linalg.Vector, expected: linalg.Vector) =>
          assert(output === expected)
      }(t.encoder)
    }
  }

  test("OneHotEncoderEstimator: encoding multiple columns and dropLast = false") {
    val data = Seq(
      Row(0.0, Vectors.sparse(3, Seq((0, 1.0))), 2.0, Vectors.sparse(4, Seq((2, 1.0)))),
      Row(1.0, Vectors.sparse(3, Seq((1, 1.0))), 3.0, Vectors.sparse(4, Seq((3, 1.0)))),
      Row(2.0, Vectors.sparse(3, Seq((2, 1.0))), 0.0, Vectors.sparse(4, Seq((0, 1.0)))),
      Row(0.0, Vectors.sparse(3, Seq((0, 1.0))), 1.0, Vectors.sparse(4, Seq((1, 1.0)))),
      Row(0.0, Vectors.sparse(3, Seq((0, 1.0))), 0.0, Vectors.sparse(4, Seq((0, 1.0)))),
      Row(2.0, Vectors.sparse(3, Seq((2, 1.0))), 2.0, Vectors.sparse(4, Seq((2, 1.0)))))

    val schema = StructType(Array(
        StructField("input1", DoubleType),
        StructField("expected1", new VectorUDT),
        StructField("input2", DoubleType),
        StructField("expected2", new VectorUDT)))

    val df = spark.createDataFrame(sc.parallelize(data), schema)

    val encoder = new OneHotEncoderEstimator()
      .setInputCols(Array("input1", "input2"))
      .setOutputCols(Array("output1", "output2"))
    assert(encoder.getDropLast === true)
    encoder.setDropLast(false)
    assert(encoder.getDropLast === false)

    val model = encoder.fit(df)
    testTransformer[(Double, linalg.Vector, Double, linalg.Vector)](
      df,
      model,
      "output1",
      "output2",
      "expected1",
      "expected2") {
      case Row(output1: linalg.Vector, output2: linalg.Vector, expected1: linalg.Vector, expected2: linalg.Vector) =>
        assert(output1 === expected1)
        assert(output2 === expected2)
    }
  }

  test("OneHotEncoderEstimator: encoding multiple columns and dropLast = true") {
    val data = Seq(
      Row(0.0, Vectors.sparse(2, Seq((0, 1.0))), 2.0, Vectors.sparse(3, Seq((2, 1.0)))),
      Row(1.0, Vectors.sparse(2, Seq((1, 1.0))), 3.0, Vectors.sparse(3, Seq[(Int, Double)]())),
      Row(2.0, Vectors.sparse(2, Seq[(Int, Double)]()), 0.0, Vectors.sparse(3, Seq((0, 1.0)))),
      Row(0.0, Vectors.sparse(2, Seq((0, 1.0))), 1.0, Vectors.sparse(3, Seq((1, 1.0)))),
      Row(0.0, Vectors.sparse(2, Seq((0, 1.0))), 0.0, Vectors.sparse(3, Seq((0, 1.0)))),
      Row(2.0, Vectors.sparse(2, Seq[(Int, Double)]()), 2.0, Vectors.sparse(3, Seq[(Int, Double)]((2, 1.0)))))

    val schema = StructType(Array(
        StructField("input1", DoubleType),
        StructField("expected1", new VectorUDT),
        StructField("input2", DoubleType),
        StructField("expected2", new VectorUDT)))

    val df = spark.createDataFrame(sc.parallelize(data), schema)

    val encoder = new OneHotEncoderEstimator()
      .setInputCols(Array("input1", "input2"))
      .setOutputCols(Array("output1", "output2"))

    val model = encoder.fit(df)
    testTransformer[(Double, linalg.Vector, Double, linalg.Vector)](
      df,
      model,
      "output1",
      "output2",
      "expected1",
      "expected2") {
      case Row(output1: linalg.Vector, output2: linalg.Vector, expected1: linalg.Vector, expected2: linalg.Vector) =>
        assert(output1 === expected1)
        assert(output2 === expected2)
    }
  }

  test("Throw error on invalid values") {
    val trainingData = Seq((0, 0), (1, 1), (2, 2))
    val trainingDF = trainingData.toDF("id", "a")
    val testData = Seq((0, 0), (1, 2), (1, 3))
    val testDF = testData.toDF("id", "a")

    val encoder = new OneHotEncoderEstimator()
      .setInputCols(Array("a"))
      .setOutputCols(Array("encoded"))

    val model = encoder.fit(trainingDF)
    testTransformerByInterceptingException[(Int, Int)](
      testDF,
      model,
      expectedMessagePart = "Unseen value: 3.0. To handle unseen values",
      firstResultCol = "encoded")

  }

  test("Can't transform on negative input") {
    val trainingDF = Seq((0, 0), (1, 1), (2, 2)).toDF("a", "b")
    val testDF = Seq((0, 0), (-1, 2), (1, 3)).toDF("a", "b")

    val encoder = new OneHotEncoderEstimator()
      .setInputCols(Array("a"))
      .setOutputCols(Array("encoded"))

    val model = encoder.fit(trainingDF)
    testTransformerByInterceptingException[(Int, Int)](
      testDF,
      model,
      expectedMessagePart = "Negative value: -1.0. Input can't be negative",
      firstResultCol = "encoded")
  }

  test("Keep on invalid values: dropLast = false") {
    val trainingDF = Seq(Tuple1(0), Tuple1(1), Tuple1(2)).toDF("input")

    val testData = Seq(
      Row(0.0, Vectors.sparse(4, Seq((0, 1.0)))),
      Row(1.0, Vectors.sparse(4, Seq((1, 1.0)))),
      Row(3.0, Vectors.sparse(4, Seq((3, 1.0)))))

    val schema = StructType(Array(
        StructField("input", DoubleType),
        StructField("expected", new VectorUDT)))

    val testDF = spark.createDataFrame(sc.parallelize(testData), schema)

    val encoder = new OneHotEncoderEstimator()
      .setInputCols(Array("input"))
      .setOutputCols(Array("output"))
      .setHandleInvalid("keep")
      .setDropLast(false)

    val model = encoder.fit(trainingDF)
    testTransformer[(Double, linalg.Vector)](testDF, model, "output", "expected") {
      case Row(output: linalg.Vector, expected: linalg.Vector) =>
        assert(output === expected)
    }
  }

  test("Keep on invalid values: dropLast = true") {
    val trainingDF = Seq(Tuple1(0), Tuple1(1), Tuple1(2)).toDF("input")

    val testData = Seq(
      Row(0.0, Vectors.sparse(3, Seq((0, 1.0)))),
      Row(1.0, Vectors.sparse(3, Seq((1, 1.0)))),
      Row(3.0, Vectors.sparse(3, Seq[(Int, Double)]())))

    val schema = StructType(Array(
        StructField("input", DoubleType),
        StructField("expected", new VectorUDT)))

    val testDF = spark.createDataFrame(sc.parallelize(testData), schema)

    val encoder = new OneHotEncoderEstimator()
      .setInputCols(Array("input"))
      .setOutputCols(Array("output"))
      .setHandleInvalid("keep")
      .setDropLast(true)

    val model = encoder.fit(trainingDF)
    testTransformer[(Double, linalg.Vector)](testDF, model, "output", "expected") {
      case Row(output: linalg.Vector, expected: linalg.Vector) =>
        assert(output === expected)
    }
  }

  test("OneHotEncoderModel changes dropLast") {
    val data = Seq(
      Row(0.0, Vectors.sparse(3, Seq((0, 1.0))), Vectors.sparse(2, Seq((0, 1.0)))),
      Row(1.0, Vectors.sparse(3, Seq((1, 1.0))), Vectors.sparse(2, Seq((1, 1.0)))),
      Row(2.0, Vectors.sparse(3, Seq((2, 1.0))), Vectors.sparse(2, Seq[(Int, Double)]())),
      Row(0.0, Vectors.sparse(3, Seq((0, 1.0))), Vectors.sparse(2, Seq((0, 1.0)))),
      Row(0.0, Vectors.sparse(3, Seq((0, 1.0))), Vectors.sparse(2, Seq((0, 1.0)))),
      Row(2.0, Vectors.sparse(3, Seq((2, 1.0))), Vectors.sparse(2, Seq[(Int, Double)]())))

    val schema = StructType(Array(
        StructField("input", DoubleType),
        StructField("expected1", new VectorUDT),
        StructField("expected2", new VectorUDT)))

    val df = spark.createDataFrame(sc.parallelize(data), schema)

    val encoder = new OneHotEncoderEstimator()
      .setInputCols(Array("input"))
      .setOutputCols(Array("output"))

    val model = encoder.fit(df)

    model.setDropLast(false)
    testTransformer[(Double, linalg.Vector, linalg.Vector)](df, model, "output", "expected1") {
      case Row(output: linalg.Vector, expected1: linalg.Vector) =>
        assert(output === expected1)
    }

    model.setDropLast(true)
    testTransformer[(Double, linalg.Vector, linalg.Vector)](df, model, "output", "expected2") {
      case Row(output: linalg.Vector, expected2: linalg.Vector) =>
        assert(output === expected2)
    }
  }

  test("OneHotEncoderModel changes handleInvalid") {
    val trainingDF = Seq(Tuple1(0), Tuple1(1), Tuple1(2)).toDF("input")

    val testData = Seq(
      Row(0.0, Vectors.sparse(4, Seq((0, 1.0)))),
      Row(1.0, Vectors.sparse(4, Seq((1, 1.0)))),
      Row(3.0, Vectors.sparse(4, Seq((3, 1.0)))))

    val schema = StructType(Array(
        StructField("input", DoubleType),
        StructField("expected", new VectorUDT)))

    val testDF = spark.createDataFrame(sc.parallelize(testData), schema)

    val encoder = new OneHotEncoderEstimator()
      .setInputCols(Array("input"))
      .setOutputCols(Array("output"))

    val model = encoder.fit(trainingDF)
    model.setHandleInvalid("error")

    testTransformerByInterceptingException[(Double, linalg.Vector)](
      testDF,
      model,
      expectedMessagePart = "Unseen value: 3.0. To handle unseen values",
      firstResultCol = "output")

    model.setHandleInvalid("keep")
    testTransformerByGlobalCheckFunc[(Double, linalg.Vector)](testDF, model, "output") { _ => }
  }

  test("Transforming on mismatched attributes") {
    val attr = NominalAttribute.defaultAttr.withValues("small", "medium", "large")
    val df = Seq(0.0, 1.0, 2.0, 1.0).map(Tuple1.apply).toDF("size")
      .select(col("size").as("size", attr.toMetadata()))
    val encoder = new OneHotEncoderEstimator()
      .setInputCols(Array("size"))
      .setOutputCols(Array("encoded"))
    val model = encoder.fit(df)

    val testAttr = NominalAttribute.defaultAttr.withValues("tiny", "small", "medium", "large")
    val testDF = Seq(0.0, 1.0, 2.0, 3.0).map(Tuple1.apply).toDF("size")
      .select(col("size").as("size", testAttr.toMetadata()))
    testTransformerByInterceptingException[(Double)](
      testDF,
      model,
      expectedMessagePart = "OneHotEncoderModel expected 2 categorical values",
      firstResultCol = "encoded")
  }
}
