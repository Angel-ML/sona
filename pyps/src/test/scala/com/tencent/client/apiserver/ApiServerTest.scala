package com.tencent.client.apiserver

import com.tencent.angel.apiserver.plasma.PlasmaClient
import com.tencent.angel.ps.apis.PSApis
import com.tencent.angel.ps.common.WorkerContext
import org.scalatest.FunSuite

class ApiServerTest extends FunSuite {

  test("register") {
    val client = new PlasmaClient("", "", 1)
    val header = new Handler(client, 1234)
    val envCtx = WorkerContext(null)
    val apis = new PSApis(envCtx, client)
    header.register(apis)
  }
}
