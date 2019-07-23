package com.tencent.angel.sona.core

import com.tencent.angel.client.AngelPSClient
import com.tencent.angel.ml.core.network.EnvContext

case class SparkEnvContext(client: AngelPSClient) extends EnvContext[AngelPSClient]
