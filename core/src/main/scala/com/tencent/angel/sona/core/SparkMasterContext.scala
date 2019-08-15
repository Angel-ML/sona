package com.tencent.angel.sona.core

import com.tencent.angel.client.AngelPSClient
import com.tencent.angel.mlcore.network.EnvContext
import com.tencent.angel.psagent.PSAgent

case class SparkMasterContext(client: AngelPSClient) extends EnvContext[AngelPSClient]


case class SparkWorkerContext(client: PSAgent) extends EnvContext[PSAgent]
