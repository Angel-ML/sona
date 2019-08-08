package org.apache.spark.angel.graph.utils

import java.text.SimpleDateFormat
import java.util.Date

import scala.util.Random
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import com.tencent.angel.conf.AngelConf
import com.tencent.angel.ml.matrix.MatrixContext
import com.tencent.angel.ml.matrix.psf.get.base.{GetFunc, GetResult}
import com.tencent.angel.ml.matrix.psf.update.base.{UpdateFunc, VoidResult}
import com.tencent.angel.ps.storage.matrix.PartitionSourceArray
import com.tencent.angel.ps.storage.partitioner.Partitioner
import com.tencent.angel.sona.models.PSMatrix
import org.apache.spark.angel.graph.utils.NEModel.NEDataSet
import org.apache.spark.angel.psf.embedding.NESlice.SliceResult
import org.apache.spark.angel.psf.embedding.{Init, InitParam, NEModelRandomize, NESlice}

abstract class NEModel(numNode: Int,
                       dimension: Int,
                       numPart: Int,
                       numNodesPerRow: Int = -1,
                       order: Int = 2,
                       useNegativeTable: Boolean = true,
                       var seed: Int = Random.nextInt()) extends Serializable {

  // partDim is the dimensions for one node in each server partition
  protected val partDim: Int = dimension / numPart
  require(dimension % numPart == 0, "dimension must be times of numPart, (dimension % numPart == 0)")
  protected val rand = new Random(seed)

  // Create one ps matrix to hold the input vectors and the output vectors for all node
  val psMatrix: PSMatrix = createPSMatrix
  val matrixId: Int = psMatrix.id

  // initialize embeddings
  randomInitialize(rand.nextInt)

  def getDotFunc(data: NEDataSet, batchSeed: Int, ns: Int, partitionId: Int): GetFunc = ???

  def getAdjustFunc(data: NEDataSet, batchSeed: Int, ns: Int, grad: Array[Float], partitionId: Int): UpdateFunc = ???

  def getInitFunc(numPartitions: Int, maxIndex: Int, maxLength: Int, negative: Int, window: Int): UpdateFunc = {
    val param = new InitParam(matrixId, numPartitions, maxIndex, maxLength, negative, order, partDim, window)
    new Init(param)
  }

  def getMatrixContext: MatrixContext

  protected def randomInitialize(seed: Int): Unit = {
    val beforeRandomize = System.currentTimeMillis()
    psfUpdate(new NEModelRandomize(matrixId, dimension / numPart, dimension, order, seed))
    NEModel.logTime(s"Model successfully Randomized, cost ${(System.currentTimeMillis() - beforeRandomize) / 1000.0}s")
  }

  protected def psfUpdate(func: UpdateFunc): VoidResult = {
    psMatrix.psfUpdate(func).get()
  }

  protected def psfGet(func: GetFunc): GetResult = {
    psMatrix.psfGet(func)
  }

  def createPSMatrix: PSMatrix = {
    // create ps matrix
    val begin = System.currentTimeMillis()
    val mc: MatrixContext = getMatrixContext

    mc.getAttributes.put(AngelConf.ANGEL_PS_PARTITION_SOURCE_CLASS,
      classOf[PartitionSourceArray].getName)

    if (mc.getAttributes.containsKey(AngelConf.Angel_PS_PARTITION_CLASS)) {
      val partitionClassName = mc.getAttributes.get(AngelConf.Angel_PS_PARTITION_CLASS)
      mc.setPartitionerClass(Class.forName(partitionClassName).asInstanceOf[Class[Partitioner]])
      mc.getAttributes.remove(AngelConf.Angel_PS_PARTITION_CLASS)
    }

    val psMatrix = PSMatrix.matrix(mc)
    NEModel.logTime(s"Model created, takes ${(System.currentTimeMillis() - begin) / 1000.0}s")

    psMatrix
  }

  def doGrad(dots: Array[Float], negative: Int, alpha: Float): Double = {
    var loss = 0.0
    for (i <- dots.indices) {
      val prob = FastSigmoid.sigmoid(dots(i))
      if (i % (negative + 1) == 0) {
        dots(i) = alpha * (1 - prob)
        loss -= FastSigmoid.log(prob)
      } else {
        dots(i) = -alpha * FastSigmoid.sigmoid(dots(i))
        loss -= FastSigmoid.log(1 - prob)
      }
    }
    loss
  }

  def destroy(): Unit = {
    psMatrix.destroy()
  }

}

object NEModel {

  def deleteIfExists(modelPath: String, ss: SparkSession): Unit = {
    val path = new Path(modelPath)
    val fs = path.getFileSystem(ss.sparkContext.hadoopConfiguration)
    if (fs.exists(path)) {
      fs.delete(path, true)
    }
  }

  def logTime(msg: String): Unit = {
    val time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date)
    println(s"[$time] $msg")
  }

  def getEmbeddingRDD(psMatrix: PSMatrix, numNode: Int, partDim: Int, order: Int): RDD[String] = {
    val startTime = System.currentTimeMillis()
    val ss = SparkSession.builder().getOrCreate()

    //.asInstanceOf[((Int, Int)) => TraversableOnce[String]]
    val rdd = slicedSavingRDDBuilder(ss, numNode, partDim).flatMap[String] { slicePair =>
      val (from, until) = slicePair
      NEModel.logTime(s"get nodes with id ranging from $from until $until")
      val getFunc = new NESlice(psMatrix.id, from, until - from, partDim, order)
      psMatrix.psfGet(getFunc).asInstanceOf[SliceResult].getSlice()
    }
    NEModel.logTime(s"saving finished, cost ${(System.currentTimeMillis() - startTime) / 1000.0}s")

    rdd
  }

  private def slicedSavingRDDBuilder(ss: SparkSession, numNode: Int, partDim: Int): RDD[(Int, Int)] = {
    val numExecutor = math.max(ss.sparkContext.statusTracker.getExecutorInfos.length - 1, 1)
    val numNodePerPartition = math.max(1, math.min(100000, (numNode - 1) / numExecutor + 1))
    val sliceBatchSize = math.min(1000000 / partDim, numNodePerPartition)
    val slices = (Array.range(0, numNode, sliceBatchSize) ++ Array(numNode))
      .sliding(2).map(arr => (arr(0), arr(1))).toArray
    val partitionNum = math.min((numNode - 1) / numNodePerPartition + 1, slices.length)
    NEModel.logTime(s"make slices: ${slices.take(5).mkString(" ")}, partitionNum: $partitionNum")
    ss.sparkContext.makeRDD(slices, partitionNum)
  }

  abstract class NEDataSet

}
