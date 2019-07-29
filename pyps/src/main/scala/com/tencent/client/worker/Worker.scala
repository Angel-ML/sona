package com.tencent.client.worker.protos

import java.io.IOException
import java.util.logging.Logger

import com.tencent.client.worker.WorkerService
import io.grpc.ServerBuilder

class Worker(val masterHost: String, val masterPort: Int, val workerPost: Int) {
  private val logger = Logger.getLogger(classOf[Worker].getSimpleName)

  private val serverBuilder = ServerBuilder.forPort(workerPost)
  private val masterService = new WorkerService(this, masterHost, masterPort)
  private val server = serverBuilder.addService(masterService).build

  @throws[IOException]
  def start(): Unit = {
    server.start
    logger.info("Server started, listening on " + masterPort)
    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run(): Unit = { // Use stderr here since the logger may have been reset by its JVM shutdown hook.
        System.err.println("*** shutting down gRPC server since JVM is shutting down")
        Worker.this.stop()
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

object Worker {
  private val logger = Logger.getLogger(Worker.getClass.getSimpleName)

  def main(args: Array[String]): Unit = {
    try {
      val server = new Worker("localhost",8980, 9005)
      server.start()
      server.blockUntilShutdown()
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }
}