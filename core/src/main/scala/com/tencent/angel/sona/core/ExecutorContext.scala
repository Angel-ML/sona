package com.tencent.angel.sona.core

import com.tencent.angel.ml.core.conf.SharedConf
import com.tencent.angel.psagent.PSAgent
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging


case class ExecutorContext(conf: SharedConf, numTask: Int,
                           partitionStat: Map[Int, Int],
                           sharedParams: Boolean = false)
  extends PSAgentContext(conf) with Serializable with Logging {

  @transient lazy val lower: Double = 1.0 / Math.log(Double.MaxValue)
  @transient lazy val upper: Double = Math.log(Double.MaxValue)

  @transient override lazy val sparkEnvContext: SparkEnvContext = SparkEnvContext(null)

  def getSamplesInPartition: Int = {
    partitionStat(TaskContext.getPartitionId())
  }
}

object ExecutorContext {
  @transient private var graphModelPoll: GraphModelPool = _
  @transient private var psAgent: PSAgent = _

  private def checkPSAgent(exeCtx: ExecutorContext): Unit = {
    if (psAgent == null) {
      psAgent = exeCtx.createAndInitPSAgent
    }
  }

  private def checkGraphModelPool(exeCtx: ExecutorContext): Unit = {
    checkPSAgent(exeCtx)

    if (graphModelPoll == null) {
      graphModelPoll = new GraphModelPool(exeCtx.sparkEnvContext, exeCtx.conf, exeCtx.numTask)
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
    graphModelPoll
  }
}
