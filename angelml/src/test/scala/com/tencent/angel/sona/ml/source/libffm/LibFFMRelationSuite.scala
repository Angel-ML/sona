/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017-2018 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package com.tencent.angel.sona.ml.source.libffm

import java.io.File
import java.nio.charset.StandardCharsets

import com.google.common.io.Files
import org.apache.spark.SparkFunSuite
import com.tencent.angel.sona.ml.util.MLlibTestSparkContext
import org.apache.spark.util.SparkUtil

class LibFFMRelationSuite extends SparkFunSuite with MLlibTestSparkContext {
  // Path for dataset
  var path: String = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    val lines0 =
      """
        |1 0:1:1.0 1:3:2.0 2:5:3.0
        |0
      """.stripMargin
    val lines1 =
      """
        |0 0:2:4.0 1:4:5.0 2:6:6.0
      """.stripMargin
    val dir = SparkUtil.createTempDir()
    val succ = new File(dir, "_SUCCESS")
    val file0 = new File(dir, "part-00000")
    val file1 = new File(dir, "part-00001")
    Files.write("", succ, StandardCharsets.UTF_8)
    Files.write(lines0, file0, StandardCharsets.UTF_8)
    Files.write(lines1, file1, StandardCharsets.UTF_8)
    path = dir.getPath
  }

  override def afterAll(): Unit = {
    try {
      val prefix = "C:\\Users\\fitzwang\\AppData\\Local\\Temp\\"
      if (path.startsWith(prefix)) {
        SparkUtil.deleteRecursively(new File(path))
      }
    } finally {
      super.afterAll()
    }
  }

  test("ffmIO"){
    val df = spark.read.format("libffm").load(path)
    val metadata = df.schema(1).metadata

    val fieldSet = MetaSummary.getFieldSet(metadata)
    println(fieldSet.mkString("[", ",", "]"))

    val keyFieldMap = MetaSummary.getKeyFieldMap(metadata)
    println(keyFieldMap.mkString("[", ",", "]"))

    df.write.format("libffm").save("temp.libffm")
  }

  test("read_ffm"){
    val df = spark.read.format("libffm").load(path)
    val metadata = df.schema(1).metadata

    val fieldSet = MetaSummary.getFieldSet(metadata)
    println(fieldSet.mkString("[", ",", "]"))

    val keyFieldMap = MetaSummary.getKeyFieldMap(metadata)
    println(keyFieldMap.mkString("[", ",", "]"))
  }

}
