package org.apache.spark.angel.graph.line

import org.apache.spark.angel.ml.Estimator
import org.apache.spark.angel.ml.param.ParamMap
import org.apache.spark.angel.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types.{IntegerType, LongType, StructField, StructType}

class LINE(override val uid: String) extends Estimator[LINEModel] with LINEParams
  with DefaultParamsWritable with Logging {

  def this() = this(Identifiable.randomUID("LINE"))

  override def fit(dataset: Dataset[_]): LINEModel = {
    val edges = dataset.select($(srcNodeIdCol), $(dstNodeIdCol)).rdd
      .map(row => (row.getInt(0), row.getInt(1)))
      .filter(f => f._1 != f._2)

    getVersion match {
      case "v1" =>
        val lineModel = new LINEPSModelV1($(embeddingMatrixName), $(maxIndex), $(embeddingDim), $(numPSPart), $(numEpoch), $(stepSize),
          $(negSample), $(batchSize), $(nodesNumPerRow), $(order), $(seed).toInt)
        lineModel.train(edges)
        new LINEModel(uid, lineModel.psMatrix)
      case "v2" =>
        val lineModel = new LINEPSModelV2($(embeddingMatrixName), $(maxIndex), $(embeddingDim), $(numPSPart), $(numEpoch), $(stepSize),
          $(negSample), $(batchSize), $(order), $(seed).toInt)
        lineModel.train(edges)
        new LINEModel(uid, lineModel.psMatrix)
      case _ => throw new Exception("Unknown Version!")
    }
  }

  override def copy(extra: ParamMap): LINE = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    // this method not transformSchema, just check schema
    validateAndTransformSchema(schema)
  }
}


object LINE extends DefaultParamsReadable[LINE] with Logging {
  override def load(path: String): LINE = super.load(path)
}