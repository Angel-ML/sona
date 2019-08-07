package org.apache.spark.angel.graph.line

import org.apache.spark.angel.graph.params._
import org.apache.spark.angel.ml.param.Params
import org.apache.spark.angel.ml.param.shared.{HasCheckpointInterval, HasNumEpoch, HasSeed, HasStepSize}
import org.apache.spark.angel.ml.util.SchemaUtils
import org.apache.spark.sql.types.StructType

trait LINEParams extends HasMaxIndex with HasPartitionNum with HasEmbeddingDim with HasNegSample with HasStepSize
  with HasBatchSize with HasNumEpoch with HasSampleRate with HasNumPSPart with HasEmbeddingMatrixName with HasOrder
  with HasNodesNumPerRow with HasNumRowDataSet with HasSeed with HasSrcNodeIdCol with HasDstNodeIdCol
  with HasOutputNodeIdCol with HasVersion with Params {

  final def setNumEpoch(epoch: Int): this.type = {
    set(numEpoch, epoch)

    this
  }

  setDefault(numEpoch, 10)

  final def setStepSize(ss: Double): this.type = {
    set(stepSize, ss)

    this
  }

  setDefault(stepSize, 0.00001)

  protected def validateAndTransformSchema(schema: StructType): StructType = {
    SchemaUtils.checkNumericType(schema, $(srcNodeIdCol))
    SchemaUtils.checkNumericType(schema, $(dstNodeIdCol))

    schema
  }
}