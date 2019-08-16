package com.tencent.angel.sona.core


import com.tencent.angel.mlcore.conf.SharedConf
import com.tencent.angel.psagent.PSAgent
import org.apache.spark.internal.CompatibleLogging
import org.apache.spark.sql.SPKSQLUtils

import scala.language.implicitConversions

case class ExecutorContext(conf: SharedConf, numTask: Int)
  extends PSAgentContext(conf) with Serializable with CompatibleLogging {

  @transient override lazy val sparkWorkerContext: SparkWorkerContext = {
    if (psAgent == null) {
      throw new Exception("Pls. startAngel first!")
    }

    SparkWorkerContext(psAgent)
  }
}

object ExecutorContext {
  @transient private var graphModelPool: GraphModelPool = _
  @transient private var psAgent: PSAgent = _

  def getPSAgent(exeCtx: ExecutorContext): PSAgent = synchronized {
    while (psAgent == null) {
      SPKSQLUtils.registerUDT()
      psAgent = exeCtx.createAndInitPSAgent
    }

    com.tencent.angel.psagent.PSAgentContext.get().getPsAgent.refreshMatrixInfo()

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
      graphModelPool = new GraphModelPool(exeCtx.sparkWorkerContext, exeCtx.numTask)
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
