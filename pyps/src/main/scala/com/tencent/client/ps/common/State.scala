package com.tencent.client.ps.common

object State extends Enumeration {
  type State = Value
  val New, Created, Initialized, Ready, Expired = Value
}