package com.tencent.angel.sona.clustering

import org.apache.spark.internal.Logging
import org.apache.spark.ml.Model
import org.apache.spark.ml.clustering.AKMeansParams
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.{MLReadable, MLReader, MLWritable, MLWriter}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types.StructType

class AngelKMeansModel(override val uid: String) extends Model[AngelKMeansModel] with AKMeansParams with MLWritable{
  override def copy(extra: ParamMap): AngelKMeansModel = defaultCopy(extra)

  override def write: MLWriter = ???

  override def transform(dataset: Dataset[_]): DataFrame = ???

  override def transformSchema(schema: StructType): StructType = ???
}

object AngelKMeansModel extends MLReadable[AngelKMeansModel] with Logging {
  override def read: MLReader[AngelKMeansModel] = ???
}
