package org.apache.spark.angel.graph.line

import org.apache.spark.angel.ml.Estimator
import org.apache.spark.angel.ml.param.ParamMap
import org.apache.spark.angel.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types.{IntegerType, LongType, StructField, StructType}

class LINEV1(override val uid: String) extends Estimator[LINEModel] with LINEParams
  with DefaultParamsWritable with Logging {

  def this() = this(Identifiable.randomUID("LINEV1"))

  override def fit(dataset: Dataset[_]): LINEModel = {
    val edges = dataset.select($(srcNodeIdCol), $(dstNodeIdCol)).rdd
      .map(row => (row.getInt(0), row.getInt(1)))
      .filter(f => f._1 != f._2)

    val lineModel = new LINEPSModel($(maxIndex), $(embeddingDim), $(numPSPart), $(numEpoch), $(stepSize), $(checkpointInterval),
      $(negSample), $(batchSize), $(nodesNumPerRow), $(order), $(seed).toInt)

    lineModel.train(edges, $(modelPath))

    new LINEModel(uid, lineModel.psMatrix)
  }

  override def copy(extra: ParamMap): LINEV1 = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    // this method not transformSchema, just check schema
    validateAndTransformSchema(schema)
  }
}


object LINEV1 extends DefaultParamsReadable[LINEV1] with Logging {
  override def load(path: String): LINEV1 = super.load(path)
}