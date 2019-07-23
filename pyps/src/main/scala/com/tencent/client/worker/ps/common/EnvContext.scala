package com.tencent.client.worker.ps.common

import com.tencent.angel.client.AngelPSClient
import com.tencent.angel.psagent.{PSAgent, PSAgentContext}
import com.tencent.client.common.protos.AsyncModel


trait EnvContext[T] {
  def client: T

  def isASP: Boolean

  def isBSP: Boolean

  def isSSP: Boolean

  def getPSAgent: PSAgent = {
    PSAgentContext.get().getPsAgent
  }

  def refreshMatrixInfo(): Unit = {
    PSAgentContext.get().getPsAgent.refreshMatrixInfo()
  }
}

case class MasterContext(override val client: AngelPSClient, asyncModel: AsyncModel) extends EnvContext[AngelPSClient] {
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


case class WorkerContext(override val client: PSAgent, asyncModel: AsyncModel) extends  EnvContext[PSAgent] {
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