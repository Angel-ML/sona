package org.apache.spark.angelml.param

import java.io.File

import com.tencent.angel.ml.core.conf.MLCoreConf
import com.tencent.angel.ml.core.utils.JsonUtils
import org.apache.hadoop.conf.Configuration
import org.apache.spark.angelml.param.shared.HasAggregationDepth
import org.json4s.JsonAST.JObject


trait AngelGraphParams extends Params with AngelDataParams with HasModelType
  with HasModelName with HasModelJson with HasAggregationDepth with ParamsHelper {

  def setModelType(value: String): this.type = setInternal(modelType, value)

  setDefault(modelType, MLCoreConf.DEFAULT_ML_MODEL_TYPE)

  def setModelName(value: String): this.type = setInternal(modelName, value)

  setDefault(modelName -> "AngelGraphModel")

  def setModelJsonFile(value: String): this.type = setInternal(modelJsonFile, value)

  def setModelJson(value: JObject): this.type = setInternal(modelJson, value)

  def setAggregationDepth(value: Int): this.type = setInternal(aggregationDepth, value)

  setDefault(aggregationDepth -> 2)

  override def updateFromJson(): this.type = {
    val jsonFile: String = getModelJsonFile
    // require(jsonFile != null && jsonFile.nonEmpty, "json file not set, please set a model json")
    val hadoopConf: Configuration = new Configuration
    if (new File(jsonFile).exists()) {
      val json: JObject = JsonUtils.parseAndUpdateJson(getModelJsonFile, sharedConf, hadoopConf)
      setModelJson(json)
      sharedConf.setJson(json)
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
  final val modelJson: JObjectParam = new JObjectParam(this, "modelJson",
    "the model's correspond json object")

  final def getModelJson: JObject = $(modelJson)

  final val modelJsonFile: Param[String] = new Param[String](this, "modelJsonFile",
    "the model json file", (value: String) => value != null && value.nonEmpty)

  final def getModelJsonFile: String = $(modelJsonFile)
}


trait HasModelType extends Params {
  final val modelType: Param[String] = new Param[String](this, "modelType",
    "the model type", (value: String) => value != null && value.nonEmpty)

  final def getModelType: String = $(modelType)
}