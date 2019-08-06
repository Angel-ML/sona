package org.apache.spark.angel.graph.line

import java.text.SimpleDateFormat
import java.util.Date

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.ml.math2.utils.RowType
import com.tencent.angel.ml.matrix.MatrixMeta
import com.tencent.angel.model.{MatrixLoadContext, MatrixSaveContext, ModelLoadContext, ModelSaveContext}
import com.tencent.angel.ps.storage.matrix.PartitionSourceArray
import com.tencent.angel.sona.context.PSContext
import com.tencent.angel.sona.models.PSMatrix
import com.tencent.angel.spark.ml.psf.embedding.NESlice
import com.tencent.angel.spark.ml.psf.embedding.NESlice.SliceResult
import org.apache.hadoop.fs.Path
import org.apache.spark.angel.graph.line
import org.apache.spark.angel.graph.line.LINEModel.logTime
import org.apache.spark.angel.ml.Model
import org.apache.spark.angel.ml.param.ParamMap
import org.apache.spark.angel.ml.util.{DefaultParamsReader, DefaultParamsWriter, MLReadable, MLReader, MLWritable, MLWriter, SchemaUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.types.StructType


class LINEModel(override val uid: String, private var psMatrix: PSMatrix) extends Model[LINEModel] with LINEParams with MLWritable {
  private val partDim: Int = getEmbeddingDim / getNumPSPart
  var matMeta: MatrixMeta = _

  if (psMatrix != null) {
    val metaOpt = PSContext.instance().getMatrixMeta(psMatrix.id)
    if (metaOpt.nonEmpty) {
      matMeta = metaOpt.get
      setModel(matMeta.getName)
    }
  }

  def this(uid: String) = {
    this(uid, null)
  }

  def setPSMatrix(psMatrix: PSMatrix): this.type = {
    this.psMatrix = psMatrix
    if (psMatrix != null) {
      val metaOpt = PSContext.instance().getMatrixMeta(psMatrix.id)
      if (metaOpt.nonEmpty) {
        matMeta = metaOpt.get
        setModel(matMeta.getName)
      }
    }

    this
  }

  def getPSMatrix: PSMatrix = {
    this.psMatrix
  }

  /**
    * Returns an `MLWriter` instance for this ML instance.
    */
  override def write: MLWriter = new line.LINEModel.LINEModelWriter(this)

  def getEmbeddingRDD: RDD[String] = {
    val ss = SparkSession.builder().getOrCreate()
    slicedSavingRDDBuilder(ss, partDim).flatMap[String](getSlice)
  }

  private def getSlice(slicePair: (Int, Int)): Array[String] = {
    val (from, until) = slicePair
    logTime(s"get nodes with id ranging from $from until $until")
    val func = new NESlice(psMatrix.id, from, until - from, partDim, getOrder)
    psMatrix.psfGet(func).asInstanceOf[SliceResult].getSlice()
  }

  private def slicedSavingRDDBuilder(ss: SparkSession, partDim: Int): RDD[(Int, Int)] = {
    val numNode = getMaxIndex
    val numExecutor = getAvailableExecutorNum(ss)
    val numNodePerPartition = math.max(1, math.min(100000, (numNode - 1) / numExecutor + 1))
    val sliceBatchSize = math.min(1000000 / partDim, numNodePerPartition)
    val slices = (Array.range(0, numNode, sliceBatchSize) ++ Array(numNode))
      .sliding(2).map(arr => (arr(0), arr(1))).toArray
    val partitionNum = math.min((numNode - 1) / numNodePerPartition + 1, slices.length)
    logTime(s"make slices: ${slices.take(5).mkString(" ")}, partitionNum: $partitionNum")
    ss.sparkContext.makeRDD(slices, partitionNum)
  }

  private def getAvailableExecutorNum(ss: SparkSession): Int = {
    math.max(ss.sparkContext.statusTracker.getExecutorInfos.length - 1, 1)
  }

  /**
    * Transforms the input dataset.
    */
  override def transform(dataset: Dataset[_]): DataFrame = {
    logInfo(s"${classOf[LINEModel].getSimpleName} cannot transform a dataset, do nothing here!")

    dataset.toDF()
  }

  /**
    * :: DeveloperApi ::
    *
    * Check transform validity and derive the output schema from the input schema.
    *
    * We check validity for interactions between parameters during `transformSchema` and
    * raise an exception if any parameter value is invalid. Parameter value checks which
    * do not depend on other parameters are handled by `Param.validate()`.
    *
    * Typical implementation should first conduct verification on schema change and parameter
    * validity, including complex parameter interaction checks.
    */
  override def transformSchema(schema: StructType): StructType = {
    logInfo(s"${classOf[LINEModel].getSimpleName} do not transformSchema, just forward!")
    schema
  }

  override def copy(extra: ParamMap): LINEModel = {
    val copied = new LINEModel(uid, psMatrix)
    copyValues(copied, extra).setParent(parent)
  }
}

object LINEModel extends MLReadable[LINEModel] {

  private def logTime(msg: String): Unit = {
    val time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date)
    println(s"[$time] $msg")
  }

  class LINEModelWriter(instance: LINEModel) extends MLWriter {
    private val psMatrix: PSMatrix = instance.psMatrix

    /**
      * `save()` handles overwriting and then calls this method.  Subclasses should override this
      * method to implement the actual saving of the instance.
      */
    override protected def saveImpl(path: String): Unit = {
      // 1. deleteIfExists
      val basePath = new Path(path)
      val ss = SparkSession.builder().getOrCreate()
      val fs = basePath.getFileSystem(ss.sparkContext.hadoopConfiguration)
      if (fs.exists(basePath)) {
        fs.delete(basePath, true)
      }

      // 2. save params
      val params = new Path(path, "params")
      logTime(s"saving model to ${params.toString}")
      val paramsWriter = new DefaultParamsWriter(instance)
      paramsWriter.save(params.toString)

      // 3. save embedding
      val embedding = new Path(path, "embedding")
      val startTime = System.currentTimeMillis()
      logTime(s"saving model to ${embedding.toString}")

      val matMeta = PSContext.instance().getMatrixMeta(psMatrix.id)
      if (matMeta.isEmpty) {
        throw new Exception("Cannot get MatrixMeta! ")
      } else {
        val name = matMeta.get.getName
        val msc = new MatrixSaveContext(name)
        val saveContext = new ModelSaveContext(embedding.toString)
        saveContext.addMatrix(msc)
        PSContext.instance().save(saveContext)
      }

      logTime(s"saving finished, cost ${(System.currentTimeMillis() - startTime) / 1000.0}s")
    }
  }

  private class LINEModelReader extends MLReader[LINEModel] {

    /**
      * Loads the ML component from the input path.
      */
    override def load(path: String): LINEModel = {
      // 1. check if exists
      val basePath = new Path(path)
      val ss = SparkSession.builder().getOrCreate()
      val fs = basePath.getFileSystem(ss.sparkContext.hadoopConfiguration)
      if (fs.exists(basePath)) {
        throw new Exception(s"${basePath.toString} is not exists")
      }

      // 2. read params
      val params = new Path(path, "params")
      logTime(s"load params from ${params.toString}")

      val paramsReader = new DefaultParamsReader[LINEModel]
      val model = paramsReader.load(params.toString)

      // 3. read embedding
      val embedding = new Path(path, "embedding")
      logTime(s"load embedding from ${embedding.toString}")
      val psContext = PSContext.instance()

      val partDim: Int = model.getEmbeddingDim / model.getNumPSPart
      val sizeOccupiedPerNode: Int = partDim * model.getOrder
      val numNode: Int = model.getMaxIndex
      val numPart: Int = model.getNumPSPart
      val numNodesPerRow: Int = model.getNodesNumPerRow
      val psMatrix = createPSMatrix(sizeOccupiedPerNode, numNode, numPart, numNodesPerRow)

      val loadCtx = new ModelLoadContext(embedding.toString)
      val matrixContext = new MatrixLoadContext(model.getModel)
      loadCtx.addMatrix(matrixContext)
      psContext.load(loadCtx)

      model.setPSMatrix(psMatrix)
    }

    private def createPSMatrix(sizeOccupiedPerNode: Int,
                               numNode: Int,
                               numPart: Int,
                               numNodesPerRow: Int = -1): PSMatrix = {
      require(numNodesPerRow <= Int.MaxValue / sizeOccupiedPerNode,
        s"size exceed Int.MaxValue, $numNodesPerRow * $sizeOccupiedPerNode > ${Int.MaxValue}")

      val rowCapacity = if (numNodesPerRow > 0) numNodesPerRow else Int.MaxValue / sizeOccupiedPerNode
      val numRow = (numNode - 1) / rowCapacity + 1
      val numNodesEachRow = if (numNodesPerRow > 0) numNodesPerRow else (numNode - 1) / numRow + 1
      val numCol = numPart.toLong * numNodesEachRow * sizeOccupiedPerNode
      val rowsInBlock = numRow
      val colsInBlock = numNodesEachRow * sizeOccupiedPerNode
      logTime(s"matrix meta:\n" +
        s"colNum: $numCol\n" +
        s"rowNum: $numRow\n" +
        s"colsInBlock: $colsInBlock\n" +
        s"rowsInBlock: $rowsInBlock\n" +
        s"numNodesPerRow: $numNodesEachRow\n" +
        s"sizeOccupiedPerNode: $sizeOccupiedPerNode"
      )
      // create ps matrix
      val begin = System.currentTimeMillis()
      val psMatrix = PSMatrix
        .dense(numRow, numCol, rowsInBlock, colsInBlock, RowType.T_FLOAT_DENSE,
          Map(AngelConf.ANGEL_PS_PARTITION_SOURCE_CLASS -> classOf[PartitionSourceArray].getName))
      logTime(s"Model created, takes ${(System.currentTimeMillis() - begin) / 1000.0}s")

      psMatrix
    }
  }

  /**
    * Returns an `MLReader` instance for this class.
    */
  override def read: MLReader[LINEModel] = new LINEModelReader
}
