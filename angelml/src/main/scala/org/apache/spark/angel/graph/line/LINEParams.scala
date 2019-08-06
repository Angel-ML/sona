package org.apache.spark.angel.graph.line

import java.util.Random

import org.apache.spark.angel.graph.params._
import org.apache.spark.angel.ml.param.shared.{HasCheckpointInterval, HasNumEpoch, HasSeed, HasStepSize}
import org.apache.spark.angel.ml.util.SchemaUtils
import org.apache.spark.sql.types.StructType

trait LINEParams extends HasMaxIndex with HasPartitionNum with HasWindowSize with HasEmbeddingDim with HasNegSample
  with HasStepSize with HasBatchSize with HasNumEpoch with HasSampleRate with HasNumPSPart with HasModelPath
  with HasModel with HasCheckpointInterval with HasOrder with HasNodesNumPerRow with HasNumRowDataSet
  with HasSeed with HasMaxLength with HasSrcNodeIdCol with HasDstNodeIdCol with HasOutputNodeIdCol
  with HasOutputCoreIdCol {

  def setCheckpointInterval(interval: Int): this.type = {
    set(checkpointInterval, interval)

    this
  }

  setDefault(checkpointInterval, -1)

  def setNumEpoch(epoch: Int): this.type = {
    set(numEpoch, epoch)

    this
  }

  setDefault(numEpoch, 10)

  def setSeed(seeds: Long): this.type = {
    set(seed, seeds)

    this
  }

  setDefault(seed, (new Random).nextInt())

  def setStepSize(ss: Double): this.type = {
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