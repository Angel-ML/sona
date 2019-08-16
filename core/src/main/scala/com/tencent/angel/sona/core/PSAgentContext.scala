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
import com.tencent.angel.sona.util.ConfUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.util.ShutdownHookManager
import org.apache.spark.TaskContext
import org.apache.spark.internal.CompatibleLogging

abstract class PSAgentContext(conf: SharedConf) extends CompatibleLogging{
  @transient protected var psAgent: PSAgent = _
  @transient protected var stopAgentHookTask: Runnable = _

  val sparkWorkerContext: SparkWorkerContext

  private def getLocalConf(taskContext: TaskContext): Configuration = {
    val taskConf = new Configuration()

    conf.allKeys().filter(key => key.startsWith("ml.") || key.startsWith("angel."))
      .foreach { key => taskConf.set(key, conf.get(key)) }

    try {
      val keys = conf.get(ConfUtils.CONF_KEYS)
      if (keys != null && keys.nonEmpty) {
        for (key <- keys.split(";")) {
          taskConf.set(key, taskContext.getLocalProperty(key))
        }
      }
    } catch {
      case e: Exception =>
        log.info(e.getMessage)
    }


    taskConf
  }

  protected def createAndInitPSAgent: PSAgent = {
    if (psAgent == null) {
      val taskContext = TaskContext.get()
      val taskConf = getLocalConf(taskContext)
      val ip = conf.get(ConfUtils.MASTER_IP)
      val port = conf.getInt(ConfUtils.MASTER_PORT)
      val contextId = TaskContext.getPartitionId()

      psAgent = new PSAgent(taskConf, ip, port, contextId, false, null)

      stopAgentHookTask = new Runnable {
        def run(): Unit = doStopAgent()
      }

      ShutdownHookManager.get().addShutdownHook(stopAgentHookTask,
        FileSystem.SHUTDOWN_HOOK_PRIORITY + 20
      )

      psAgent.initAndStart()
    }

    psAgent
  }

  def isAgentAlive: Boolean = synchronized {
    if (psAgent != null) true else false
  }

  def stopAgent(): Unit = synchronized {
    if (stopAgentHookTask != null) {
      ShutdownHookManager.get().removeShutdownHook(stopAgentHookTask)
      stopAgentHookTask = null
    }

    doStopAgent()
  }

  private def doStopAgent(): Unit = {
    if (psAgent != null) {
      psAgent.stop()
      psAgent = null
    }
  }

  def getPSAgent: PSAgent = psAgent
}
