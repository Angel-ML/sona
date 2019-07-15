package com.tencent.angel.ps.common

import com.tencent.angel.client.AngelClient
import com.tencent.angel.psagent.PSAgent

trait EnvContext[T] {
  def client: T
}

case class MasterContext(override val client: AngelClient) extends  EnvContext[AngelClient]
case class WorkerContext(override val client: PSAgent) extends  EnvContext[PSAgent]