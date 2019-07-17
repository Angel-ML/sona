package com.tencent.client.worker

import com.tencent.angel.psagent.{PSAgent, PSAgentContext}
import com.tencent.client.common.Executor
import com.tencent.client.ps.common.{EnvContext, WorkerContext}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.util.ShutdownHookManager
import org.apache.spark.internal.Logging

class Worker private(conf: Configuration, ip: String, port: Int, contextId: Int) extends Executor with Logging {
  private var psAgent: PSAgent = _

  private var stopAgentHookTask = new Runnable {
    def run(): Unit = doStop()
  }

  def start(): Unit = synchronized {
    if (psAgent == null) {
      psAgent = new PSAgent(conf, ip, port, contextId, false, null)
      ShutdownHookManager.get().addShutdownHook(stopAgentHookTask,
        FileSystem.SHUTDOWN_HOOK_PRIORITY + 20
      )
      psAgent.initAndStart()
    }
  }

  def getPSAgent: PSAgent = psAgent

  def stop(): Unit = synchronized {
    if (stopAgentHookTask != null) {
      ShutdownHookManager.get().removeShutdownHook(stopAgentHookTask)
      stopAgentHookTask = null
    }

    doStop()
  }

  private def doStop(): Unit = {
    if (psAgent != null) {
      psAgent.stop()
      psAgent = null
    }
  }

  def refreshMatrixInfo(): Unit = {
    val agent = PSAgentContext.get().getPsAgent
    assert(agent != null && psAgent != null)
    if (agent == psAgent) {
      psAgent.refreshMatrixInfo()
    } else {
      logInfo("the PSAgent is not the one created in worker!")
      agent.refreshMatrixInfo()
    }
  }

  override def context: EnvContext[_] = {
    if (psAgent == null) {
      logWarning("psAgent is empty, please start master first!")
      null
    } else {
      WorkerContext(psAgent)
    }
  }
}

object Worker {
  private var worker: Worker = _

  def get(hadoopConf: Configuration, ip: String, port: Int, contextId: Int): Worker = synchronized {
    if (worker == null) {
      worker = new Worker(hadoopConf, ip, port, contextId)
    }

    worker
  }

  def get: Worker = synchronized {
    if (worker != null) {
      throw new Exception("please init master first")
    }

    worker
  }
}
