package org.apache.spark.angel.graph.word2vec

import java.util.Random

import org.apache.spark.angel.graph.params._
import org.apache.spark.angel.ml.param.shared._
import org.apache.spark.angel.ml.util.SchemaUtils
import org.apache.spark.sql.types.StructType

trait Word2VecParams extends HasMaxIndex with HasPartitionNum with HasWindowSize with HasEmbeddingDim with HasNegSample
  with HasStepSize with HasBatchSize with HasNumEpoch with HasSampleRate with HasNumPSPart with HasModelPath
  with HasModel with HasCheckpointInterval with HasNodesNumPerRow with HasNumRowDataSet
  with HasSeed with HasMaxLength with HasInput with HasEmbeddingMatrixName {

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


  def setStepSize(ss: Double): this.type = {
    set(stepSize, ss)

    this
  }

  setDefault(stepSize, 0.00001)

  protected def validateAndTransformSchema(schema: StructType): StructType = {
    SchemaUtils.checkNumericType(schema, $(input))
    schema
  }
}