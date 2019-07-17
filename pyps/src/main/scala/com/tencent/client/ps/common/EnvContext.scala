package com.tencent.client.ps.common

import com.tencent.angel.client.AngelPSClient
import com.tencent.angel.psagent.PSAgent

trait EnvContext[T] {
  def client: T
}

case class MasterContext(override val client: AngelPSClient) extends  EnvContext[AngelPSClient]
case class WorkerContext(override val client: PSAgent) extends  EnvContext[PSAgent]