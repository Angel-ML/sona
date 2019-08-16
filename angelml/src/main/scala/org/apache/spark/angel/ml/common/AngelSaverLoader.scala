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
package org.apache.spark.angel.ml.common

import com.tencent.angel.client.AngelPSClient
import com.tencent.angel.mlcore.conf.SharedConf
import com.tencent.angel.sona.core.DriverContext
import org.apache.hadoop.fs.Path
import org.apache.spark.angel.ml.util.{DefaultParamsReader, DefaultParamsWriter, MLReader, MLUtils, MLWriter}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row

import scala.reflect.ClassTag

object AngelSaverLoader {

  case class ModelData(sharedConfStr: String, angelModelName: String) {
    def this(conf: SharedConf, angelModelName: String) {
      this(conf.toString(), angelModelName)
    }

    lazy val conf: SharedConf = SharedConf.fromString(sharedConfStr)
  }

  private[angel] class AngelModelWriter[Model <: AngelSparkModel](instance: Model)
    extends MLWriter with Serializable with Logging {
    override protected def saveImpl(path: String): Unit = {
      // 1. Save metadata and Params
      DefaultParamsWriter.saveMetadata(instance, path, sc)

      // 2. save angel model to s"angel_${instance.modelName}"
      val angelModelPath = new Path(path, "angel").toString
      instance.angelModel.saveModel(DriverContext.get().sparkMasterContext,
        MLUtils.getHDFSPath(angelModelPath))

      // for park2.1
      // cancel saving other information for the moment due to the error below:
      // ERROR ApplicationMaster: User class threw exception: java.util.ServiceConfigurationError:
      // org.apache.spark.sql.sources.DataSourceRegister: Provider org.apache.spark.sql.execution.datasources.csv.CSVFileFormat
      // could not be instantiated
      //java.util.ServiceConfigurationError: org.apache.spark.sql.sources.DataSourceRegister:
      // Provider org.apache.spark.sql.execution.datasources.csv.CSVFileFormat could not be instantiated

      // 3. prepare other information to save
      val modelData = ModelData(instance.sharedConf.toString(), instance.angelModelName)

      // 4. save other info to parquet file
      sparkSession.createDataFrame(Seq(modelData)).repartition(1)
        .write.parquet(new Path(path, "data").toString)
    }
  }

  private[angel] class AngelModelReader[Model <: AngelSparkModel : ClassTag](implicit psClient: AngelPSClient)
    extends MLReader[Model] with Serializable with Logging {
    private val clz = implicitly[ClassTag[Model]].runtimeClass

    override def load(path: String): Model = {
      // 1. load metadata
      val metadata = DefaultParamsReader.loadMetadata(path, sc, clz.getName)

      // 2. read data from parquet in "path/data"
      val dataPath = new Path(path, "data").toString
      val data = sparkSession.read.format("parquet").load(dataPath)
      val Row(confString: String, modelName: String) = data.select("sharedConfStr", "angelModelName").head()

      val cstr = clz.getConstructor(classOf[String], classOf[String])
      val model = cstr.newInstance(metadata.uid, modelName).asInstanceOf[Model]

      val sharedConf = SharedConf.fromString(confString)
      val angelModelPath = new Path(path, "angel").toString
      val sparkEnvContext = model.sparkEnvContext
      model.angelModel.updateConf(sharedConf)
      model.angelModel
        .buildNetwork()
        .createMatrices(sparkEnvContext)
        .loadModel(sparkEnvContext, MLUtils.getHDFSPath(angelModelPath), null)

      // 3. set Params from metadata
      metadata.getAndSetParams(model)
      model
    }
  }

}
