package com.tencent.angel.sona.core

import java.io.File

import com.tencent.angel.client.{AngelContext, AngelPSClient}
import com.tencent.angel.conf.AngelConf
import com.tencent.angel.ml.core.conf.SharedConf
import com.tencent.angel.ml.core.utils.JsonUtils
import com.tencent.angel.sona.util.ConfUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.util.ShutdownHookManager
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SPKSQLUtils
import org.apache.spark.{SparkConf, SparkException}

import scala.collection.mutable

class DriverContext private(val sharedConf: SharedConf, val hadoopConf: Configuration) extends PSAgentContext(sharedConf) {
  private var angelContext: AngelContext = _
  private var angelClient: AngelPSClient = _
  private var stopAngelHookTask: Runnable = _

  private val bcVariables = new mutable.HashSet[Broadcast[_]]()
  private val cachedRDDs = new mutable.HashSet[RDD[_]]()

  @transient override lazy val sparkEnvContext: SparkEnvContext = {
    if (angelClient == null) {
      throw new Exception("Pls. startAngel first!")
    }

    SparkEnvContext(angelClient)
  }

  private lazy val driverId: String = java.util.UUID.randomUUID.toString

  def getId: String = driverId

  def registerBroadcastVariables(bcVar: Broadcast[_]): this.type = synchronized {
    bcVariables.add(bcVar)
    this
  }

  def registerCachedRDD(rdd: RDD[_]): this.type = {
    cachedRDDs.add(rdd)
    this
  }

  def getAngelClient: AngelPSClient = {
    angelClient
  }

  def startAngelAndPSAgent(): AngelPSClient = synchronized {
    SPKSQLUtils.registerUDT()

    if (angelClient == null) {
      angelClient = new AngelPSClient(hadoopConf)

      val iter = hadoopConf.iterator()
      while (iter.hasNext) {
        val entry = iter.next()
        println(s"${entry.getKey} -> ${entry.getValue}")
      }

      stopAngelHookTask = new Runnable {
        def run(): Unit = doStopAngel()
      }

      ShutdownHookManager.get().addShutdownHook(stopAngelHookTask,
        FileSystem.SHUTDOWN_HOOK_PRIORITY + 10)

      angelContext = angelClient.startPS()
      hadoopConf.set(ConfUtils.MASTER_IP, angelContext.getMasterLocation.getIp)
      hadoopConf.set(ConfUtils.MASTER_PORT, angelContext.getMasterLocation.getPort.toString)

      ConfUtils.merge(sharedConf, hadoopConf,
        ConfUtils.MASTER_IP, ConfUtils.MASTER_PORT, "angel", "ml", "spark.ps", "spark.hadoop")
    }

    createAndInitPSAgent

    angelClient
  }

  def isAngelAlive: Boolean = synchronized {
    if (angelClient != null) true else false
  }

  def stopAngelAndPSAgent(): Unit = synchronized {
    if (stopAngelHookTask != null) {
      ShutdownHookManager.get().removeShutdownHook(stopAngelHookTask)
      stopAngelHookTask = null
    }

    stopAgent()
    doStopAngel()
  }

  private def doStopAngel(): Unit = {
    if (bcVariables.nonEmpty) {
      bcVariables.foreach { bcVar: Broadcast[_] =>
        if (bcVar != null) {
          try {
            bcVar.destroy()
          } catch {
            case spe: SparkException =>
              throw spe
            case e: Exception =>
              throw e
          }
        }
      }

      bcVariables.clear()
    }

    if (angelClient != null) {
      angelClient.stopPS()
      angelClient = null
    }
  }
}

object DriverContext {
  private var driverContext: DriverContext = _

  private def initConf(conf: Configuration): SharedConf = {
    val sharedConf = new SharedConf

    // 1. parse json and update conf
    if (conf.get(AngelConf.ANGEL_ML_CONF) != null) {
      var jsonFileName = conf.get(AngelConf.ANGEL_ML_CONF)
      val validateFileName = if (new File(jsonFileName).exists()) {
        jsonFileName
      } else {
        val splits = jsonFileName.split(File.separator)
        jsonFileName = splits(splits.length - 1)

        val file = new File(jsonFileName)
        if (file.exists()) {
          jsonFileName
        } else {
          println("File not found! ")
          ""
        }
      }

      if (!validateFileName.isEmpty) {
        JsonUtils.parseAndUpdateJson(validateFileName, sharedConf, conf)
      }
    }

    // 2. add configure on Hadoop Configuration
    val iter = conf.iterator()
    while (iter.hasNext) {
      val entry = iter.next()
      val key = entry.getKey
      val value = entry.getValue

      if (key.startsWith("ml.") || key.startsWith("angel.")) {
        sharedConf.set(key, value)
      }
    }

    sharedConf
  }

  def get(conf: SparkConf): DriverContext = synchronized {
    if (driverContext == null) {
      val hadoopConf = ConfUtils.convertToHadoop(conf)
      driverContext = new DriverContext(initConf(hadoopConf), hadoopConf)
    }

    driverContext
  }

  def get(): DriverContext = synchronized {
    require(driverContext != null, "driverContext is null")
    driverContext
  }

  def adjustExecutorJVM(conf: SparkConf): SparkConf = {
    val extraOps = conf.getOption("spark.ps.executor.extraJavaOptions")
    val defaultOps = conf.get("spark.executor.extraJavaOptions", "")

    val extraOpsStr = if (extraOps.isDefined) {
      extraOps.get
    } else {
      var executorMemSizeInMB = conf.getSizeAsMb("spark.executor.memory", "2048M")
      if (executorMemSizeInMB < 2048) executorMemSizeInMB = 2048

      val isUseDirect: Boolean = conf.getBoolean("spark.ps.usedirectbuffer", defaultValue = true)
      val maxUse = executorMemSizeInMB - 512
      var directRegionSize: Int = 0
      if (isUseDirect) directRegionSize = (maxUse * 0.3).toInt
      else directRegionSize = (maxUse * 0.2).toInt
      val heapMax = maxUse - directRegionSize
      val youngRegionSize = (heapMax * 0.3).toInt
      val survivorRatio = 4

      conf.set("spark.executor.memory", heapMax + "M")

      val executorOps = new StringBuilder()
        .append(" -Xmn").append(youngRegionSize).append("M")
        .append(" -XX:MaxDirectMemorySize=").append(directRegionSize).append("M")
        .append(" -XX:SurvivorRatio=").append(survivorRatio)
        .append(" -XX:+AggressiveOpts")
        .append(" -XX:+UseLargePages")
        .append(" -XX:+UseConcMarkSweepGC")
        .append(" -XX:CMSInitiatingOccupancyFraction=50")
        .append(" -XX:+UseCMSInitiatingOccupancyOnly")
        .append(" -XX:+CMSScavengeBeforeRemark")
        .append(" -XX:+UseCMSCompactAtFullCollection")
        .append(" -verbose:gc")
        .append(" -XX:+PrintGCDateStamps")
        .append(" -XX:+PrintGCDetails")
        .append(" -XX:+PrintCommandLineFlags")
        .append(" -XX:+PrintTenuringDistribution")
        .append(" -XX:+PrintAdaptiveSizePolicy")
        .toString()
      executorOps
    }
    conf.set("spark.executor.extraJavaOptions", defaultOps + " " + extraOpsStr)
  }
}
