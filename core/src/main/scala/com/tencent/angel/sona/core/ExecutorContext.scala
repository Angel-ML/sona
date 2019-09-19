/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017-2018 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */
package com.tencent.angel.sona.core


import com.tencent.angel.mlcore.conf.SharedConf
import com.tencent.angel.psagent.PSAgent
import org.apache.spark.internal.CompatibleLogging
import org.apache.spark.sql.SPKSQLUtils

import scala.language.implicitConversions

case class ExecutorContext(conf: SharedConf, numTask: Int)
  extends PSAgentContext(conf) with CompatibleLogging with Serializable{

  @transient override lazy val sparkWorkerContext: SparkWorkerContext = {
    if (psAgent == null) {
      throw new Exception("Pls. startAngel first!")
    }

    SparkWorkerContext(psAgent)
  }
}

object ExecutorContext {
  @transient private var graphModelPool: GraphModelPool = _
  @transient private var psAgent: PSAgent = _

  def getPSAgent(exeCtx: ExecutorContext): PSAgent = synchronized {
    while (psAgent == null) {
      SPKSQLUtils.registerUDT()
      psAgent = exeCtx.createAndInitPSAgent
    }

    com.tencent.angel.psagent.PSAgentContext.get().getPsAgent.refreshMatrixInfo()

    psAgent
  }

  def stopPSAgent(): Unit = synchronized {
    while (psAgent != null) {
      psAgent.stop()
      psAgent = null
    }
  }

  private def checkGraphModelPool(exeCtx: ExecutorContext): Unit = {
    getPSAgent(exeCtx)

    if (graphModelPool == null) {
      graphModelPool = new GraphModelPool(exeCtx.sparkWorkerContext, exeCtx.numTask)
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
    graphModelPool
  }
}
