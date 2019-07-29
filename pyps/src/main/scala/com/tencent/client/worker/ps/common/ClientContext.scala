package com.tencent.client.worker.ps.common

import com.tencent.angel.client.{AngelContext, AngelPSClient}
import com.tencent.angel.common.location.Location
import com.tencent.angel.psagent.{PSAgent, PSAgentContext}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.util.ShutdownHookManager


class ClientContext(workerId: Long, conf: Configuration) {
  private var angelClient: AngelPSClient = _
  private var stopAngelHookTask: Runnable = _
  private var masterLoc: Location = _

  def startAngel(): AngelPSClient = synchronized {
    if (angelClient == null) {
      angelClient = new AngelPSClient(conf)

      stopAngelHookTask = new Runnable {
        def run(): Unit = doStopAngel()
      }

      ShutdownHookManager.get().addShutdownHook(stopAngelHookTask,
        FileSystem.SHUTDOWN_HOOK_PRIORITY + 10)

      val angelContext = angelClient.startPS()
      masterLoc = angelContext.getMasterLocation
    }

    angelClient
  }

  def isAngelAlive: Boolean = synchronized {
    if (angelClient != null) true else false
  }

  def stopAngel(): Unit = synchronized {
    if (stopAngelHookTask != null) {
      ShutdownHookManager.get().removeShutdownHook(stopAngelHookTask)
      stopAngelHookTask = null
    }

    doStopAngel()
  }

  private def doStopAngel(): Unit = {
    if (angelClient != null) {
      angelClient.stopPS()
      angelClient = null
    }
  }

  def getAngelClient: AngelPSClient = angelClient

  def setMasterLocation(loc: Location): this.type = {
    masterLoc = loc

    this
  }

  def getMasterLocation: Location = masterLoc

  //---------------------------------------------------------------
  protected var psAgent: PSAgent = _
  protected var stopAgentHookTask: Runnable = _

  def startPSAgent(): PSAgent = {
    if (psAgent == null) {
      val ip: String = masterLoc.getIp
      val port: Int = masterLoc.getPort
      psAgent = new PSAgent(conf, ip, port, workerId.toInt, false, null)

      stopAgentHookTask = new Runnable {
        def run(): Unit = doStopAgent()
      }

      ShutdownHookManager.get().addShutdownHook(stopAgentHookTask,
        FileSystem.SHUTDOWN_HOOK_PRIORITY + 20
      )

      psAgent.initAndStart()
    }

    psAgent
  }

  def isPSAgentAlive: Boolean = synchronized {
    if (psAgent != null) true else false
  }

  def stopPSAgent(): Unit = synchronized {
    if (stopAgentHookTask != null) {
      ShutdownHookManager.get().removeShutdownHook(stopAgentHookTask)
      stopAgentHookTask = null
    }

    doStopAgent()
  }

  private def doStopAgent(): Unit = {
    if (psAgent != null) {
      psAgent.stop()
      psAgent = null
    }
  }

  def getPSAgent: PSAgent = psAgent

  def refreshMatrixInfo(): Unit = {
    PSAgentContext.get().getPsAgent.refreshMatrixInfo()
  }
}
