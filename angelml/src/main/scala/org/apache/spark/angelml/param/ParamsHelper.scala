package org.apache.spark.angelml.param

import com.tencent.angel.client.AngelPSClient
import com.tencent.angel.mlcore.conf.SharedConf
import com.tencent.angel.sona.util.ConfUtils


trait ParamsHelper extends Params {
  var finalized: Boolean = false
  val sharedConf: SharedConf

  def setInternal[T](param: Param[T], value: T): this.type = {
    if (!finalized) {
      set(param, value)
    } else {
      throw new Exception("the estimator is finalized, cannot set!")
    }

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
