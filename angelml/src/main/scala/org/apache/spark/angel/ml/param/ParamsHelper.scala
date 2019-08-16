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
package org.apache.spark.angel.ml.param

import com.tencent.angel.client.AngelPSClient
import com.tencent.angel.mlcore.conf.SharedConf
import com.tencent.angel.sona.util.ConfUtils


trait ParamsHelper extends Params {
  var finalized: Boolean = false
  val sharedConf: SharedConf

  def setInternal[T](param: Param[T], value: T): this.type = {
//    if (!finalized) {
//      set(param, value)
//    } else {
//      throw new Exception("the estimator is finalized, cannot set!")
//    }
    set(param, value)
    this
  }

  def updateFromCMDConf(psClient: AngelPSClient): this.type = {
    if (psClient != null) {
      ConfUtils.merge(sharedConf, psClient.getConf,
        "angel", "ml", ConfUtils.MASTER_IP, ConfUtils.MASTER_PORT, ConfUtils.TOTAL_CORES)
    }

    this
  }

  def updateFromJson(): this.type

  def updateFromProgramSetting(): this.type = {
    this
  }

  def finalizeConf(psClient: AngelPSClient): this.type = synchronized {
    if (!finalized) {
      // 1. get the params from Angel Client
      updateFromCMDConf(psClient)

      // 2. get the params from json file
      updateFromJson()

      // 3. get the params from user setting in the program
      updateFromProgramSetting()

      finalized = true
    }

    this
  }
}
