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

package org.apache.spark.angel.ml.util

import java.io.File

import org.apache.spark.SparkContext
import org.apache.spark.sql.types.UDTRegistration
import org.apache.spark.sql.{SQLContext, SQLImplicits, SparkSession}
import org.apache.spark.util.Utils
import org.scalatest.Suite

trait MLlibTestSparkContext extends TempDirectory { self: Suite =>
  @transient var spark: SparkSession = _
  @transient var sc: SparkContext = _
  @transient var checkpointDir: String = _

  override def beforeAll() {
    super.beforeAll()

    UDTRegistration.register("org.apache.spark.angelml.linalg.Vector", "org.apache.spark.angelml.linalg.VectorUDT")
    UDTRegistration.register("org.apache.spark.angelml.linalg.DenseVector", "org.apache.spark.angelml.linalg.VectorUDT")
    UDTRegistration.register("org.apache.spark.angelml.linalg.SparseVector", "org.apache.spark.angelml.linalg.VectorUDT")
    UDTRegistration.register("org.apache.spark.angelml.linalg.Matrix", "org.apache.spark.angelml.linalg.MatrixUDT")
    UDTRegistration.register("org.apache.spark.angelml.linalg.DenseMatrix", "org.apache.spark.angelml.linalg.MatrixUDT")
    UDTRegistration.register("org.apache.spark.angelml.linalg.SparseMatrix", "org.apache.spark.angelml.linalg.MatrixUDT")

    spark = SparkSession.builder
      .master("local[2]")
      .appName("MLlibUnitTest")
      .getOrCreate()
    sc = spark.sparkContext

    checkpointDir = Utils.createDirectory(tempDir.getCanonicalPath, "checkpoints").toString
    sc.setCheckpointDir(checkpointDir)
  }

  override def afterAll() {
    try {
      Utils.deleteRecursively(new File(checkpointDir))
      SparkSession.clearActiveSession()
      if (spark != null) {
        spark.stop()
      }
      spark = null
    } finally {
      super.afterAll()
    }
  }

  /**
   * A helper object for importing SQL implicits.
   *
   * Note that the alternative of importing `spark.implicits._` is not possible here.
   * This is because we create the `SQLContext` immediately before the first test is run,
   * but the implicits import is needed in the constructor.
   */
  protected object testImplicits extends SQLImplicits {
    protected override def _sqlContext: SQLContext = self.spark.sqlContext
  }
}
