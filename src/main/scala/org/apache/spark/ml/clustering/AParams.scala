package org.apache.spark.ml.clustering

import org.apache.spark.ml.linalg.VectorUDT
import org.apache.spark.ml.param.{IntParam, ParamValidators, Params}
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util.SchemaUtils
import org.apache.spark.sql.types.{IntegerType, StructType}

trait AKMeansParams extends Params with HasMaxIter with HasFeaturesCol
  with HasSeed with HasPredictionCol with HasTol {
  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  setDefault(featuresCol, "features")

  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  setDefault(predictionCol, "prediction")

  protected def validateAndTransformSchema(schema: StructType): StructType = {
    SchemaUtils.checkColumnType(schema, $(featuresCol), new VectorUDT)
    SchemaUtils.appendColumn(schema, $(predictionCol), IntegerType)
  }
}


trait HasNumCluster extends Params {
  final val k = new IntParam(this, "k", "The number of clusters to create. " +
    "Must be > 1.", ParamValidators.gt(1))

  def getK: Int = $(k)
}