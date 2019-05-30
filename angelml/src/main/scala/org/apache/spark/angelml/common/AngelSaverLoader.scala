package org.apache.spark.angelml.common

import com.tencent.angel.client.AngelPSClient
import com.tencent.angel.ml.core.conf.SharedConf
import com.tencent.angel.sona.core.DriverContext
import org.apache.hadoop.fs.Path
import org.apache.spark.angelml.util._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row

import scala.reflect.ClassTag

object AngelSaverLoader {

  case class ModelData(sharedConfStr: String, angelModelName: String) {
    def this(conf: SharedConf, angelModelName: String) {
      this(conf.toString(), angelModelName)
    }

    lazy val conf: SharedConf = SharedConf.fromString(sharedConfStr)
  }

  private[angelml] class AngelModelWriter[Model <: AngelSparkModel](instance: Model)
    extends MLWriter with Serializable with Logging {
    override protected def saveImpl(path: String): Unit = {
      // 1. Save metadata and Params
      DefaultParamsWriter.saveMetadata(instance, path, sc)

      // 2. save angel model to s"angel_${instance.modelName}"
      val angelModelPath = new Path(path, "angel").toString
      instance.angelModel.saveModel(DriverContext.get().sparkEnvContext,
        MLUtils.getHDFSPath(angelModelPath))

      // 3. prepare other information to save
      val modelData = ModelData(instance.sharedConf.toString(), instance.angelModelName)

      // 4. save other info to parquet file
      sparkSession.createDataFrame(Seq(modelData)).repartition(1)
        .write.parquet(new Path(path, "data").toString)
    }
  }

  private[angelml] class AngelModelReader[Model <: AngelSparkModel : ClassTag](implicit psClient: AngelPSClient)
    extends MLReader[Model] with Serializable with Logging {
    private val clz = implicitly[ClassTag[Model]].runtimeClass

    override def load(path: String): Model = {
      // 1. load metadata
      val metadata = DefaultParamsReader.loadMetadata(path, sc, clz.getName)

      // 2. read data from parquet in "path/data"
      val dataPath = new Path(path, "data").toString
      val data = sparkSession.read.format("parquet").load(dataPath)
      val Row(confString: String, modelName: String) = data.select("sharedConfStr", "angelModelName").head()
      SharedConf.fromString(confString)

      val cstr = clz.getConstructor(classOf[String], classOf[String])
      val model = cstr.newInstance(metadata.uid, modelName).asInstanceOf[Model]

      val angelModelPath = new Path(path, "angel").toString
      val sparkEnvContext = model.sparkEnvContext
      model.angelModel
        .buildNetwork()
        .createMatrices(sparkEnvContext)
        .loadModel(sparkEnvContext, MLUtils.getHDFSPath(angelModelPath), null)

      // 3. set Params from metadata
      metadata.getAndSetParams(model)
      model
    }
  }

}
