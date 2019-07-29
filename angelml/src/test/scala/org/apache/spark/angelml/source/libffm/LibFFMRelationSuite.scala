package org.apache.spark.angelml.source.libffm

import java.io.File
import java.nio.charset.StandardCharsets

import com.google.common.io.Files
import org.apache.spark.SparkFunSuite
import org.apache.spark.angelml.util.MLlibTestSparkContext
import org.apache.spark.util.Utils

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
    val dir = Utils.createTempDir()
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
        Utils.deleteRecursively(new File(path))
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
