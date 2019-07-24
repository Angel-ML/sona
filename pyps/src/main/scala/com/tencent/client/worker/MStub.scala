package com.tencent.client.worker

import java.net.InetAddress
import java.util

import com.tencent.angel.common.location.Location
import com.tencent.client.common.Utils
import com.tencent.client.common.protos.{AsyncModel, CompleteTaskReq, VoidReq}
import com.tencent.client.master.protos._
import io.grpc.ManagedChannelBuilder
import org.apache.hadoop.conf.Configuration

import scala.collection.mutable

class MStub(host: String, port: Int) {
  private val channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext.build
  private val blockingStub = AngelCleintMasterGrpc.newBlockingStub(channel)
  // private val asyncStub = AngelCleintMasterGrpc.newStub(channel)

  var workId: Long = 0

  @throws[Exception]
  def registerWorker(): WorkerInfo = synchronized {
    val host = InetAddress.getLocalHost.getHostAddress
    val registerWorkerReq = RegisterWorkerReq.newBuilder()
      .setHost(host)
      .setPort(port)
      .build()
    val resp = blockingStub.registerWorker(registerWorkerReq)
    if (resp.getRet == 0) {
      throw new Exception(s"Worker[$host:$port] has registered! ")
    }
    val conf = new Configuration()
    Utils.fillConf(resp.getConfMap, conf)

    workId = resp.getWorkId
    WorkerInfo(workId, resp.getIsChief, resp.getAsyncModel, resp.getHeartBeatInterval, conf)
  }

  def setAngelLocation(loc: Location): Unit = synchronized {
    val req = SetAngelLocationReq.newBuilder()
      .setWorkId(workId)
      .setHost(loc.getIp)
      .setPort(loc.getPort)
      .build()

    blockingStub.setAngelLocation(req)
  }

  def getAngelLocation: Location = synchronized {
    val req = VoidReq.newBuilder().setItemId(workId).build()
    val resp = blockingStub.getAngelLocation(req)
    new Location(resp.getHost, resp.getPort)
  }

  def registerTask: TaskInfo = synchronized {
    val req = RegisterTaskReq.newBuilder()
      .setWorkId(workId)
      .setTimestamp(System.currentTimeMillis())
      .build()
    val resp = blockingStub.registerTask(req)
    val taskId = resp.getTaskId
    val numTask = resp.getNumTask

    val iter = resp.getClockMap.entrySet().iterator()
    val list = new mutable.ListBuffer[(Long, Int)]()
    while (iter.hasNext) {
      val entry = iter.next()
      val key: Long = entry.getKey
      val value = entry.getValue
      list.append(key -> value)
    }

    val clock = list.toMap

    TaskInfo(taskId, numTask, clock)
  }

  def clock(taskId: Long, currClock: Int, batchSize: Int): Map[Long, Int] = synchronized {
    val clockReq = ClockReq.newBuilder()
      .setTaskId(taskId)
      .setClock(currClock)
      .setBatchSize(batchSize)
      .build()
    val resp = blockingStub.clock(clockReq)

    val iter = resp.getClockMapMap.entrySet().iterator()
    val list = new mutable.ListBuffer[(Long, Int)]()
    while (iter.hasNext) {
      val entry = iter.next()
      val key: Long = entry.getKey
      val value = entry.getValue
      list.append(key -> value)
    }

    list.toMap
  }

  def getClockMap(taskId: Long): Map[Long, Int] = synchronized {
    val clockMapReq = GetClockMapReq.newBuilder().setTaskId(taskId).build()

    val resp = blockingStub.getClockMap(clockMapReq)

    val iter = resp.getClockMapMap.entrySet().iterator()
    val list = new mutable.ListBuffer[(Long, Int)]()
    while (iter.hasNext) {
      val entry = iter.next()
      val key: Long = entry.getKey
      val value = entry.getValue
      list.append(key -> value)
    }

    list.toMap
  }

  def completeTask(taskId: Long): Unit = synchronized {
    val req = CompleteTaskReq.newBuilder()
      .setTaskId(taskId)
      .setTimestamps(System.currentTimeMillis())
      .build()

    blockingStub.completeTask(req)
  }

  def getGlobalBatchSize: Int = synchronized {
    val req = VoidReq.newBuilder().setItemId(workId).build()
    val resp = blockingStub.getGlobalBatchSize(req)
    resp.getBatchSize
  }

  def sendHeartBeat: Command = synchronized {
    val req = HeartBeatReq.newBuilder().setWorkId(workId).build()
    val resp = blockingStub.heartBeat(req)
    resp.getCmd
  }

  def shutdown(): Unit = synchronized {
    channel.shutdown()
    while (!channel.isShutdown) {
      Thread.sleep(100)
    }
  }
}
