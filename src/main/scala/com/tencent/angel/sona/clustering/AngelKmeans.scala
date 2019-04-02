package com.tencent.angel.sona.clustering

import org.apache.spark.ml.Estimator
import org.apache.spark.ml.clustering.AKMeansParams
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types.StructType

class AngelKmeans(override val uid: String) extends Estimator[AngelKMeansModel]
  with AKMeansParams with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("AngelKmeans"))

  override def fit(dataset: Dataset[_]): AngelKMeansModel = ???

  override def copy(extra: ParamMap): Estimator[AngelKMeansModel] = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }
}


object AngelKmeans extends DefaultParamsReadable[AngelKmeans] {

  override def load(path: String): AngelKmeans = super.load(path)
}