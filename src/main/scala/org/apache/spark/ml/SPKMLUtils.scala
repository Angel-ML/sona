package org.apache.spark.ml

import com.tencent.angel.sona.core.DriverContext
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.io.SparkHadoopWriterUtils
import org.apache.spark.ml.param.Params
import org.apache.spark.ml.util.DefaultParamsReader.Metadata
import org.apache.spark.ml.util.{DefaultParamsReader, DefaultParamsWriter}
import org.json4s.{JObject, JValue}

object SPKMLUtils {
  def saveMetadata(instance: Params,
                   path: String,
                   sc: SparkContext,
                   extraMetadata: Option[JObject] = None,
                   paramMap: Option[JValue] = None): Unit = {
    DefaultParamsWriter.saveMetadata(instance, path, sc, extraMetadata, paramMap)
  }

  def loadMetadata(path: String, sc: SparkContext, expectedClassName: String = ""): Metadata = {
    DefaultParamsReader.loadMetadata(path, sc, expectedClassName)
  }

  def getAndSetParams(instance: Params,
                      metadata: Metadata,
                      skipParams: Option[List[String]] = None): Unit = {
    DefaultParamsReader.getAndSetParams(instance, metadata, skipParams)
  }

  def distory(bcVar:Broadcast[_]): Unit = {
    if (bcVar != null && bcVar.isValid) {
      bcVar.destroy(true)
    }
  }

  def getHDFSPath(path: String): String = {
    val conf = DriverContext.get().getAngelClient.getConf
    SparkHadoopWriterUtils.createPathFromString(
      path, new JobConf(conf)).toString
  }
}
