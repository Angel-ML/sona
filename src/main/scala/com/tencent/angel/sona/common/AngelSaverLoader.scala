package com.tencent.angel.sona.common

import com.tencent.angel.client.AngelPSClient
import com.tencent.angel.ml.core.conf.SharedConf
import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.ml.SPKMLUtils
import org.apache.spark.ml.util.{MLReader, MLWriter}
import com.tencent.angel.sona.core.DriverContext
import com.tencent.angel.sona.utils.ConfUtils
import org.apache.spark.sql.Row

import scala.reflect.ClassTag

object AngelSaverLoader {

  case class ModelData(sharedConfStr: String, angelModelName: String) {
    def this(conf: SharedConf, angelModelName: String) {
      this(conf.toString(), angelModelName)
    }

    lazy val conf: SharedConf = SharedConf.fromString(sharedConfStr)
  }

  private[sona] class AngelModelWriter[Model <: AngelSparkModel](instance: Model)
    extends MLWriter with Logging {
    override protected def saveImpl(path: String): Unit = {
      // 1. Save metadata and Params
      SPKMLUtils.saveMetadata(instance, path, sc)

      // 2. save angel model to s"angel_${instance.modelName}"
      val angelModelPath = new Path(path, "angel").toString
      instance.angelModel.saveModel(DriverContext.get().sparkEnvContext,
        SPKMLUtils.getHDFSPath(angelModelPath))

      // 3. prepare other information to save
      val modelData = ModelData(instance.sharedConf.toString(), instance.angelModelName)

      // 4. save other info to parquet file
      sparkSession.createDataFrame(Seq(modelData)).repartition(1)
        .write.parquet(new Path(path, "data").toString)
    }
  }

  private[sona] class AngelModelReader[Model <: AngelSparkModel : ClassTag](implicit psClient: AngelPSClient)
    extends MLReader[Model] with Logging {
    private val clz = implicitly[ClassTag[Model]].runtimeClass

    override def load(path: String): Model = {
      DriverContext.get().createAndInitPSAgent

      // 1. load metadata
      val metadata = SPKMLUtils.loadMetadata(path, sc, clz.getName)

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
        .loadModel(sparkEnvContext, SPKMLUtils.getHDFSPath(angelModelPath))

      // 3. set Params from metadata
      SPKMLUtils.getAndSetParams(model, metadata)
      model
    }
  }
}
