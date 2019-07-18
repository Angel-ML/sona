package com.tencent.client.common

import com.tencent.angel.psagent.{PSAgent, PSAgentContext}
import com.tencent.client.ps.common.EnvContext


object AsyncModel extends Enumeration {
  type AsyncModel = Value
  val BSP, ASP, SSP = Value
}

trait Executor {

  def start(): Unit

  def stop(): Unit

  def context: EnvContext[_]

  def getPSAgent: PSAgent

  def isASP: Boolean

  def isBSP: Boolean

  def isSSP: Boolean
}

object Executor {
  def getPSAgent: PSAgent = {
    PSAgentContext.get().getPsAgent
  }
}
