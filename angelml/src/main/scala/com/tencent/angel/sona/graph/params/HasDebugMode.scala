package com.tencent.angel.sona.graph.params

import com.tencent.angel.sona.ml.param.{BooleanParam, Params}

trait HasDebugMode extends Params {

  final val debugMode = new BooleanParam(this, "debugMode", "debugMode")

  final def setDebugMode(mode: Boolean): this.type = set(debugMode, mode)

  final def getDebugMode: Boolean = $(debugMode)
}
