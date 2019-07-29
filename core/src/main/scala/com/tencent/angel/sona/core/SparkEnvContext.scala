package com.tencent.angel.sona.core

import com.tencent.angel.client.AngelPSClient
import com.tencent.angel.mlcore.network.EnvContext

case class SparkEnvContext(client: AngelPSClient) extends EnvContext[AngelPSClient]
