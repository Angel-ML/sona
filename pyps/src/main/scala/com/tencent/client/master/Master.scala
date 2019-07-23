package com.tencent.client.master

import io.grpc.ServerBuilder
import java.io.IOException
import java.util
import java.util.concurrent.atomic.AtomicInteger
import java.util.logging.Logger

import com.tencent.client.common.protos.AsyncModel



class Master(val port: Int, val numTask: Int, val syncModel: AsyncModel, val conf: util.Map[String, String]) {
  private val logger = Logger.getLogger(classOf[Master].getSimpleName)

  private val serverBuilder = ServerBuilder.forPort(port)
  private val masterService = new MasterService(numTask, syncModel, conf)
  private val server = serverBuilder.addService(masterService).build

  @throws[IOException]
  def start(): Unit = {
    server.start
    logger.info("Server started, listening on " + port)
    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run(): Unit = { // Use stderr here since the logger may have been reset by its JVM shutdown hook.
        System.err.println("*** shutting down gRPC server since JVM is shutting down")
        Master.this.stop()
        System.err.println("*** server shut down")
      }
    })
  }

  def stop(): Unit = {
    if (server != null) server.shutdown
  }

  @throws[InterruptedException]
  private def blockUntilShutdown(): Unit = {
    if (server != null) server.awaitTermination()
  }
}

object Master {
  private val logger = Logger.getLogger(Master.getClass.getSimpleName)

  def main(args: Array[String]): Unit = {
    val server = new Master(8980, 5, AsyncModel.BSP, null)
    server.start()
    server.blockUntilShutdown()
  }
}