package com.tencent.angel.sona.core

import com.tencent.angel.ml.core.conf.SharedConf
import com.tencent.angel.psagent.PSAgent
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging


case class ExecutorContext(conf: SharedConf, numTask: Int)
  extends PSAgentContext(conf) with Serializable with Logging {

  @transient override lazy val sparkEnvContext: SparkEnvContext = SparkEnvContext(null)
}

object ExecutorContext {
  @transient private var graphModelPool: GraphModelPool = _
  @transient private var psAgent: PSAgent = _

  def getPSAgent(exeCtx: ExecutorContext): PSAgent = synchronized {
    while (psAgent == null) {
      psAgent = exeCtx.createAndInitPSAgent
    }

    psAgent
  }

  def stopPSAgent(): Unit = synchronized {
    while (psAgent != null) {
      psAgent.stop()
      psAgent = null
    }
  }

  private def checkGraphModelPool(exeCtx: ExecutorContext): Unit = {
    getPSAgent(exeCtx)

    if (graphModelPool == null) {
      graphModelPool = new GraphModelPool(exeCtx.sparkEnvContext, exeCtx.numTask)
    }
  }

  def getPSAgent: PSAgent = synchronized{
    if (psAgent != null) {
      psAgent
    } else {
      throw new Exception("psAgent is empty, pls. init first!")
    }
  }

  implicit def toGraphModelPool(exeCtx: ExecutorContext): GraphModelPool = synchronized {
    checkGraphModelPool(exeCtx)
    graphModelPool
  }
}
