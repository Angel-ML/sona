package com.tencent.client.common

import com.tencent.angel.psagent.{PSAgent, PSAgentContext}
import com.tencent.client.ps.common.EnvContext

trait Executor {
  def start(): Unit

  def stop(): Unit

  def context: EnvContext[_]

  def getPSAgent: PSAgent
}

object Executor {
  def getPSAgent: PSAgent = {
    PSAgentContext.get().getPsAgent
  }
}
