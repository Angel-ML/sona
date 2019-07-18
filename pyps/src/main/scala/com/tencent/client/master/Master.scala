package com.tencent.client.master

import com.tencent.angel.client.{AngelContext, AngelPSClient}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.util.ShutdownHookManager
import com.tencent.angel.common.location.Location
import com.tencent.angel.psagent.{PSAgent, PSAgentContext}
import com.tencent.client.common.AsyncModel.AsyncModel
import com.tencent.client.common.{AsyncModel, Executor}
import com.tencent.client.ps.common.{EnvContext, MasterContext, WorkerContext}
import org.apache.spark.internal.Logging

class Master private(val conf: Configuration, asyncModel: AsyncModel) extends Executor with Logging {
  var angelClient: AngelPSClient = _

  private var angelContext: AngelContext = _

  private var psAgent: PSAgent = _

  private var stopAngelHookTask: Runnable = new Runnable {
    def run(): Unit = doStopAngel()
  }

  private var stopAgentHookTask = new Runnable {
    def run(): Unit = doStopAgent()
  }

  override def context: EnvContext[_] = {
    if (angelClient == null) {
      logWarning("angelClient is empty, please start master first!")
      null
    } else {
      MasterContext(angelClient, asyncModel)
    }
  }

  def getMasterContext: MasterContext = {
    if (angelClient == null) {
      logWarning("angelClient is empty, please start master first!")
      null
    } else {
      MasterContext(angelClient, asyncModel)
    }
  }

  def getWorkerContext: WorkerContext = {
    if (psAgent == null) {
      logWarning("psAgent is empty, please start psAgent first!")
      null
    } else {
      WorkerContext(psAgent, asyncModel)
    }
  }

  def location: Location = {
    if (angelContext == null) {
      logWarning("angelContext is null, please start ps first!")
      null
    } else {
      angelContext.getMasterLocation
    }
  }

  def start(): Unit = {
    angelClient = new AngelPSClient(conf)
    ShutdownHookManager.get().addShutdownHook(stopAngelHookTask,
      FileSystem.SHUTDOWN_HOOK_PRIORITY + 10)
    angelContext = angelClient.startPS()
  }

  def stop(): Unit = {
    if (stopAgentHookTask != null) {
      ShutdownHookManager.get().removeShutdownHook(stopAgentHookTask)
      stopAgentHookTask = null
    }

    if (stopAngelHookTask != null) {
      ShutdownHookManager.get().removeShutdownHook(stopAngelHookTask)
      stopAngelHookTask = null
    }

    if (angelClient != null) {
      angelClient.stopPS()
      angelClient = null
    }

    if (psAgent != null) {
      psAgent.stop()
    }
  }

  def kill(): Unit = {
    if (stopAgentHookTask != null) {
      ShutdownHookManager.get().removeShutdownHook(stopAgentHookTask)
      stopAgentHookTask = null
    }

    if (psAgent != null) {
      psAgent.stop()
    }

    angelClient.killPS()
  }

  def isAngelAlive: Boolean = synchronized {
    if (angelClient != null) true else false
  }

  override def getPSAgent: PSAgent = synchronized {
    if (psAgent == null) {
      val ip: String = location.getIp
      val port: Int = location.getPort
      psAgent = new PSAgent(conf, ip, port, 0, false, null)

      ShutdownHookManager.get().addShutdownHook(stopAgentHookTask,
        FileSystem.SHUTDOWN_HOOK_PRIORITY + 20)

      psAgent.initAndStart()
    }

    psAgent
  }

  def refreshMatrixInfo(): Unit = {
    val agent = PSAgentContext.get().getPsAgent
    if (agent == psAgent) {
      psAgent.refreshMatrixInfo()
    } else {
      logInfo("the PSAgent is not the one created in master!")
      agent.refreshMatrixInfo()
    }
  }

  private def doStopAngel(): Unit = {
    if (angelClient != null) {
      angelClient.stopPS()
      angelClient = null
    }
  }

  private def doStopAgent(): Unit = {
    if (psAgent != null) {
      psAgent.stop()
    }
  }

  override def isASP: Boolean = {
    asyncModel == AsyncModel.ASP
  }

  override def isBSP: Boolean = {
    asyncModel == AsyncModel.BSP
  }

  override def isSSP: Boolean = {
    asyncModel == AsyncModel.SSP
  }
}

object Master {
  private var master: Master = _

  def get(hadoopConf: Configuration, asyncModel: AsyncModel): Master = synchronized {
    if (master == null) {
      master = new Master(hadoopConf, asyncModel)
    }

    master
  }

  def get: Master = synchronized {
    if (master != null) {
      throw new Exception("please init master first")
    }

    master
  }
}
