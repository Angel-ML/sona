package com.tencent.angel.ps.apis

import com.tencent.angel.apiserver.plasma.PlasmaClient
import com.tencent.angel.apiserver.{FuncId, HandlerId, Request, Response}
import com.tencent.angel.ps.common.EnvContext

@HandlerId(1)
class PSApis[T](envCtx: EnvContext[T], client: PlasmaClient) {

  @FuncId(1)
  def create(req: Request): Response = {

  }

  @FuncId(1)
  def init(req: Request): Response = {

  }

  @FuncId(1)
  def load(req: Request): Response = {

  }

  @FuncId(1)
  def pull(req: Request): Response = {

  }

  @FuncId(1)
  def push(req: Request): Response = {

  }

  @FuncId(1)
  def save(req: Request): Response = {

  }
}
