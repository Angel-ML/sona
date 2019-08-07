package org.apache.spark.angel.graph.line

import java.io.IOException

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.ml.math2.utils.RowType
import com.tencent.angel.ml.matrix.{MatrixContext, MatrixMeta}
import com.tencent.angel.model.output.format.{MatrixFilesMeta, ModelFilesConstent}
import com.tencent.angel.model.{MatrixLoadContext, MatrixSaveContext, ModelLoadContext, ModelSaveContext}
import com.tencent.angel.ps.storage.matrix.PartitionSourceArray
import com.tencent.angel.ps.storage.partitioner.Partitioner
import com.tencent.angel.sona.context.PSContext
import com.tencent.angel.sona.models.PSMatrix
import org.apache.hadoop.fs.{FSDataInputStream, Path}
import org.apache.spark.SparkContext
import org.apache.spark.angel.graph.line
import org.apache.spark.angel.graph.utils.{MatrixMetaUtils, NEModel}
import org.apache.spark.angel.ml.Model
import org.apache.spark.angel.ml.param.ParamMap
import org.apache.spark.angel.ml.util.{DefaultParamsReader, DefaultParamsWriter, FileSystemOverwrite, MLReadable, MLReader, MLWritable, MLWriter}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.types.StructType
import org.apache.spark.angel.graph.utils.NEModel._

class LINEModel(override val uid: String, private var psMatrix: PSMatrix) extends Model[LINEModel] with LINEParams with MLWritable {
  private val partDim: Int = getEmbeddingDim / getNumPSPart
  var matMeta: MatrixMeta = _

  if (psMatrix != null) {
    val metaOpt = PSContext.instance().getMatrixMeta(psMatrix.id)
    if (metaOpt.nonEmpty) {
      matMeta = metaOpt.get
      setEmbeddingMatrixName(matMeta.getName)
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
        setEmbeddingMatrixName(matMeta.getName)
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
    getVersion match {
      case "v1" =>
        NEModel.getEmbeddingRDD(psMatrix, getMaxIndex, partDim, getOrder)
      case _ => throw new Exception("getEmbeddingRDD not supported!")
    }
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
        val msc = new MatrixSaveContext(name, classOf[TextLINEModelOutputFormat].getTypeName)
        new FileSystemOverwrite().handleOverwrite(embedding.toString, shouldOverwrite, sc)

        val qualifiedOutputPath = if (embedding.toString.startsWith("hdfs:/")) {
          embedding
        } else {
          getQualifiedOutputPath(embedding.toString, sc)
        }
        if (fs.exists(qualifiedOutputPath)) {
          if (shouldOverwrite) {
            logInfo(s"Path $path already exists. It will be overwritten.")
            // TODO: Revert back to the original content if save is not successful.
            fs.delete(qualifiedOutputPath, true)
          } else {
            throw new IOException(s"Path $path already exists. To overwrite it, " +
              s"please use write.overwrite().save(path) for Scala and use " +
              s"write().overwrite().save(path) for Java and Python.")
          }
        }

        val saveContext = new ModelSaveContext(qualifiedOutputPath.toString)
        saveContext.addMatrix(msc)

        PSContext.getOrCreate(SparkContext.getOrCreate()).save(saveContext)
      }

      logTime(s"saving finished, cost ${(System.currentTimeMillis() - startTime) / 1000.0}s")
    }

    def getQualifiedOutputPath(path: String, sc: SparkContext): Path = {
      val hadoopConf = sc.hadoopConfiguration
      val outputPath = new Path(path)
      val fs = outputPath.getFileSystem(hadoopConf)
      outputPath.makeQualified(fs.getUri, fs.getWorkingDirectory)
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

      val embedMatPath = new Path(embedding.toString, model.getEmbeddingMatrixName)
      val mc = MatrixMetaUtils.readMatrixContext(embedMatPath, fs)
      val psMatrix = PSMatrix.matrix(mc)

      val loadCtx = new ModelLoadContext(embedding.toString)
      val matrixContext = new MatrixLoadContext(mc.getName)
      loadCtx.addMatrix(matrixContext)
      PSContext.instance().load(loadCtx)

      model.setPSMatrix(psMatrix)
    }
  }

  /**
    * Returns an `MLReader` instance for this class.
    */
  override def read: MLReader[LINEModel] = new LINEModelReader

}
