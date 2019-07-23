package com.tencent.client.worker.ps.common

object State extends Enumeration {
  type State = Value
  val New, Created, Initialized, Ready, Expired = Value
}