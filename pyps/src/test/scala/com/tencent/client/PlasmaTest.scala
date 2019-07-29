package com.tencent.client

import java.util.concurrent.TimeUnit

import com.tencent.client.plasma.PlasmaClient
import org.scalatest.FunSuite

class PlasmaTest extends FunSuite {
  test("PlasmaClient") {
    val cmd = "/home/fitz/working/arrow/plasma_store_server"
    val store = "/tmp/plasma"
    val memoryGB = 1
    PlasmaClient.startObjectStore(cmd, store, memoryGB)
    PlasmaClient.load()
    val client = PlasmaClient.get
    try {
      TimeUnit.MILLISECONDS.sleep(5000)
    } catch {
      case e: InterruptedException  =>
        e.printStackTrace();
    }

  }
}
