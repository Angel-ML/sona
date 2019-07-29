package com.tencent.client.master

import io.grpc.ServerBuilder
import java.io.IOException
import java.util
import java.util.logging.Logger

import com.tencent.client.common.protos.AsyncModel
import scala.collection.JavaConversions._



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
    val conf = Map(
      "angel.output.path.deleteonexist" -> "true",
      "angel.ps.class" -> "com.tencent.angel.ps.ParameterServer",
      "angel.ps.memory.gb" -> "4",
      "angel.job.libjars" -> "",
      "angel.job.name" -> "AngelClassification-ps",
      "angel.ps.number" -> "1",
      "angel.deploy.mode" -> "LOCAL",
      "angel.am.log.level" -> "INFO",
      "angel.psagent.cache.sync.timeinterval.ms" -> "100000000",
      "angel.ps.heartbeat.interval.ms" -> "200",
      "angel.running.mode" -> "ANGEL_PS",
      "angel.ps.total.cores" -> "1",
      "angel.ps.cpu.vcores" -> "1",
      "angel.ps.log.level" -> "INFO",
      "angel.log.path" -> "file:///home/fitz/github/fitzwang/sona/pyps/logpath",
      "angel.save.model.path" -> "file:///home/fitz/github/fitzwang/sona/pyps/modelpath",
      "plasma.store.path" -> "/home/fitz/working/arrow/plasma_store_server",
      "plasma.store.suffix" -> "/tmp/plasma",
      "plasma.store.memoryGB" -> "1",
      "python.script.name" -> "test.py"
    )

    val server = new Master(8980, 5, AsyncModel.BSP, conf)
    server.start()
    server.blockUntilShutdown()
  }
}