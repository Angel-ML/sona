package com.tencent.angel.sona.util

import java.util.UUID

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.conf.AngelConf._
import com.tencent.angel.mlcore.conf.SharedConf
import com.tencent.angel.ps.ParameterServer
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging


import scala.collection.mutable.ArrayBuffer

object ConfUtils extends Logging {
  val MASTER_IP = "angel.master.ip"
  val MASTER_PORT = "angel.master.port"
  val TOTAL_CORES = "angel.ps.total.cores"
  val CONF_KEYS = "angel.conf.keys"
  val ALGO_TYPE = "angel.algo.type"


  def update(sharedConf: SharedConf, conf: Configuration): SharedConf = {
    sharedConf.allKeys().foreach{ key =>
      try {
        val value: String = conf.get(key)
        if (value != null && value.nonEmpty) {
          sharedConf.set(key, value)
        }
      } catch {
        case _: Exception =>
      }
    }

    sharedConf
  }

  def update(sharedConf: SharedConf, conf: SparkConf): SharedConf = {
    val (keys, _) = conf.getAll.unzip
    val keySet = keys.toSet
    sharedConf.allKeys().foreach{
      case key if keySet.contains(key) =>
        val value = conf.get(key)
        if (value != null && value.nonEmpty) {
          sharedConf.set(key, value)
        }
      case _ =>
    }

    sharedConf
  }

  def merge(sharedConf: SharedConf, conf: SparkConf, filterExp: String*): SharedConf = {
    conf.getAll.foreach{
      case (key: String, value: String) if filterExp.isEmpty || filterExp.exists(expr => key.startsWith(expr)) =>
        sharedConf.set(key, value)
      case _ =>
    }

    sharedConf
  }

  def merge(sharedConf: SharedConf, conf: Configuration, filterExp: String*): SharedConf = {
    val iter = conf.iterator()

    while(iter.hasNext) {
      val entry = iter.next()
      val key = entry.getKey
      val value = entry.getValue
      if (value != null && value.nonEmpty) {
        if (filterExp.isEmpty || filterExp.exists(expr => key.startsWith(expr))) {
          sharedConf.set(key, value)
        }
      }
    }

    sharedConf
  }

  def convertToHadoop(conf: SparkConf): Configuration = {
    val appName = conf.get("spark.app.name") + "-ps"
    val queue = conf.get("spark.yarn.queue", "root.default")

    /** mode: YARN or LOCAL */
    val master = conf.getOption("spark.master")
    val isLocal = if (master.isEmpty || master.get.toLowerCase.startsWith("local")) true else false
    val deployMode = if (isLocal) "LOCAL" else conf.get("spark.ps.mode", DEFAULT_ANGEL_DEPLOY_MODE)

    val psNum = conf.getInt("spark.ps.instances", 1)
    val psCores = conf.getInt("spark.ps.cores", 1)
    val psMem = conf.getSizeAsGb("spark.ps.memory", "4g").toInt
    val psOpts = conf.getOption("spark.ps.extraJavaOptions")

    val psJars = conf.get("spark.ps.jars", "")
    val psLogLevel = conf.get("spark.ps.log.level", "INFO")
    val psClass = conf.get("spark.ps.class", classOf[ParameterServer].getName)

    val defaultFS = conf.get("spark.hadoop.fs.defaultFS", "file://")
    val tempPath = defaultFS + "/tmp/spark-on-angel/" + UUID.randomUUID()

    val psOutOverwrite = conf.getBoolean("spark.ps.out.overwrite", defaultValue = true)
    val psOutTmpOption = conf.getOption("spark.ps.out.tmp.path.prefix")


    val hadoopConf = SparkHadoopUtil.get.newConfiguration(conf)
    val filterExp = List("spark.ps", "spark.hadoop", "angel", "ml")
    conf.getAll.foreach{
      case (key: String, value: String) if filterExp.exists(expr => key.startsWith(expr)) =>
        hadoopConf.set(key, value)
      case _ =>
    }

    // setting running mode, app name, queue and deploy mode
    hadoopConf.set(ANGEL_RUNNING_MODE, "ANGEL_PS")
    hadoopConf.set(ANGEL_JOB_NAME, appName)
    hadoopConf.set(ANGEL_QUEUE, queue)
    hadoopConf.set(ANGEL_DEPLOY_MODE, deployMode)

    // For local mode, we set heartbeat a small value for fast debugging
    if (deployMode == "LOCAL")
      hadoopConf.set(ANGEL_PS_HEARTBEAT_INTERVAL_MS, "200")

    if (psOpts.isDefined)
      hadoopConf.set(ANGEL_PS_JAVA_OPTS, psOpts.get)

    // Set the temp path as the angel.save.model.path to fake the angel-ps system
    // The action type is also a fake setting.
    hadoopConf.set(ANGEL_ACTION_TYPE, "train")
    hadoopConf.set(ANGEL_SAVE_MODEL_PATH, tempPath)

    // Setting resource
    hadoopConf.setInt(ANGEL_PS_NUMBER, psNum)
    hadoopConf.setInt(ANGEL_PS_CPU_VCORES, psCores)
    hadoopConf.setInt(ANGEL_PS_MEMORY_GB, psMem)
    hadoopConf.setInt(TOTAL_CORES, psNum * psCores)

    hadoopConf.set(ANGEL_AM_LOG_LEVEL, psLogLevel)
    hadoopConf.set(ANGEL_PS_LOG_LEVEL, psLogLevel)
    hadoopConf.set(ANGEL_PS_CLASS, psClass)
    hadoopConf.set(ANGEL_JOB_LIBJARS, psJars)

    hadoopConf.setBoolean(ANGEL_JOB_OUTPUT_PATH_DELETEONEXIST, psOutOverwrite)
    psOutTmpOption.foreach(hadoopConf.set(ANGEL_JOB_TMP_OUTPUT_PATH_PREFIX, _))

    // No need for sync clock values, we don't need ssp in Spark.
    hadoopConf.setInt(ANGEL_PSAGENT_CACHE_SYNC_TIMEINTERVAL_MS, 100000000)
    hadoopConf.set(ANGEL_LOG_PATH, tempPath)

    // add user resource files
    addUserResourceFiles(conf, hadoopConf)

    hadoopConf
  }

  private def addUserResourceFiles(sparkConf: SparkConf, conf: Configuration): Unit = {
    val appStagingBaseDir = sparkConf.getOption("spark.yarn.stagingDir")
      .fold(FileSystem.get(conf).getHomeDirectory)(new Path(_))
    val appStagingDir = new Path(appStagingBaseDir, s".sparkStaging/${sparkConf.getAppId}")
    val resourceFiles = new ArrayBuffer[String]()
    sparkConf.getOption("spark.yarn.dist.files").foreach { fileList =>
      fileList.split(",").foreach { file =>
        val fileName = file.trim.split("/").last
        if (fileName.nonEmpty) {
          resourceFiles.append(new Path(appStagingDir, fileName).toString)
        }
      }
      conf.set(AngelConf.ANGEL_APP_USER_RESOURCE_FILES, resourceFiles.mkString(","))
    }
  }
}
