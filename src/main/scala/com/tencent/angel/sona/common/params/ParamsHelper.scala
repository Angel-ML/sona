package com.tencent.angel.sona.common.params

import com.tencent.angel.client.AngelPSClient
import com.tencent.angel.ml.core.conf.SharedConf
import org.apache.spark.ml.param.{Param, Params}
import com.tencent.angel.sona.utils.ConfUtils

trait ParamsHelper extends Params {
  var finalized: Boolean = false
  val sharedConf: SharedConf = SharedConf.get()

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

  def finalizeConf(psClient: AngelPSClient): this.type = {
    // 1. get the params from Angel Client
    updateFromCMDConf(psClient)

    // 2. get the params from json file
    updateFromJson()

    // 3. get the params from user setting in the program
    updateFromProgramSetting()

    finalized = true

    this
  }
}
