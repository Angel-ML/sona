package org.apache.spark.angel.ml.param

import java.io.File

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.mlcore.conf.MLCoreConf
import com.tencent.angel.mlcore.utils.{JsonUtils, MLException}
import org.apache.hadoop.conf.Configuration

trait AngelGraphParams extends Params with AngelDataParams with HasModelType
  with HasModelName with HasModelJson with ParamsHelper {

  def setModelType(value: String): this.type = setInternal(modelType, value)

  setDefault(modelType, MLCoreConf.DEFAULT_ML_MODEL_TYPE)

  def setModelName(value: String): this.type = setInternal(modelName, value)

  setDefault(modelName -> "AngelGraphModel")

  def setModelJsonFile(value: String): this.type = setInternal(modelJsonFile, value)

  override def updateFromJson(): this.type = {
    try {
      var jsonFile: String = getModelJsonFile
      // require(jsonFile != null && jsonFile.nonEmpty, "json file not set, please set a model json")

      if (jsonFile == null || jsonFile.isEmpty) {
        jsonFile = sharedConf.get(AngelConf.ANGEL_ML_CONF)
      }

      val hadoopConf: Configuration = new Configuration
      if (new File(jsonFile).exists()) {
        JsonUtils.parseAndUpdateJson(getModelJsonFile, sharedConf, hadoopConf)
      } else {
        throw MLException("json file not exists!")
      }

    } catch {
      case e: Exception =>
        e.printStackTrace()
        throw e
      case ae: AssertionError =>
        ae.printStackTrace()
        throw ae
    }


    this
  }
}


trait HasModelName extends Params {
  final val modelName: Param[String] = new Param[String](this, "modelName",
    "the name of angel model", (value: String) => value != null && value.nonEmpty)

  final def getModelName: String = $(modelName)
}


trait HasModelJson extends Params {
  final val modelJsonFile: Param[String] = new Param[String](this, "modelJsonFile",
    "the model json file", (value: String) => value != null && value.nonEmpty)

  final def getModelJsonFile: String = $(modelJsonFile)
}


trait HasModelType extends Params {
  final val modelType: Param[String] = new Param[String](this, "modelType",
    "the model type", (value: String) => value != null && value.nonEmpty)

  final def getModelType: String = $(modelType)
}