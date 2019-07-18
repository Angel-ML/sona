package com.tencent.client.ps.common

import com.tencent.angel.client.AngelPSClient
import com.tencent.angel.psagent.PSAgent
import com.tencent.client.common.AsyncModel
import com.tencent.client.common.AsyncModel.AsyncModel

trait EnvContext[T] {
  def client: T

  def isASP: Boolean

  def isBSP: Boolean

  def isSSP: Boolean
}

case class MasterContext(override val client: AngelPSClient, asyncModel: AsyncModel) extends  EnvContext[AngelPSClient] {
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