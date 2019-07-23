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
  var isChief: Boolean = false
  var asyncModel: AsyncModel = AsyncModel.BSP
  val conf: Configuration = new Configuration

  def registerWorker(): Unit = {
    val registerWorkerReq = RegisterWorkerReq.newBuilder()
      .setHost(InetAddress.getLocalHost.getHostAddress)
      .setPort(port)
      .build()
    val resp = blockingStub.registerWorker(registerWorkerReq)

    workId = resp.getWorkId
    isChief = resp.getIsChief
    asyncModel = resp.getAsyncModel
    Utils.fillConf(resp.getConfMap, conf)
  }

  def setAngelLocation(loc: Location): Unit = {
    val req = SetAngelLocationReq.newBuilder()
      .setWorkId(workId)
      .setHost(loc.getIp)
      .setPort(loc.getPort)
      .build()

    blockingStub.setAngelLocation(req)
  }

  def getAngelLocation: Location = {
    val req = VoidReq.newBuilder().setItemId(workId).build()
    val resp = blockingStub.getAngelLocation(req)
    new Location(resp.getHost, resp.getPort)
  }

  def registerTask: TaskInfo = {
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

  def clock(taskId: Long, currClock: Int, batchSize: Int): Map[Long, Int] = {
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

  def getClockMap(taskId: Long): Map[Long, Int] = {
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

  def completeTask(taskId: Long): Unit = {
    val req = CompleteTaskReq.newBuilder()
      .setTaskId(taskId)
      .setTimestamps(System.currentTimeMillis())
      .build()

    blockingStub.completeTask(req)
  }

  def getGlobalBatchSize: Int = {
    val req = VoidReq.newBuilder().setItemId(workId).build()
    val resp = blockingStub.getGlobalBatchSize(req)
    resp.getBatchSize
  }

  def sendHeartBeat: Command = {
    val req = HeartBeatReq.newBuilder().setWorkId(workId).build()
    val resp = blockingStub.heartBeat(req)
    resp.getCmd
  }

  def shutdown(): Unit = {
    channel.shutdown()
    while (!channel.isShutdown) {
      Thread.sleep(100)
    }
  }
}
