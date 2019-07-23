package com.tencent.client.master

import java.util
import java.util.concurrent.{ConcurrentHashMap, Executors}
import java.util.logging.Logger

import com.tencent.angel.common.location.Location
import com.tencent.client.common.protos.{AsyncModel, CompleteTaskReq, VoidReq, VoidResp}
import io.grpc.stub.StreamObserver
import com.tencent.client.master.protos.{GetGlobalBatchResp, _}
import java.util.concurrent.atomic.AtomicInteger
import java.lang.{Integer => JINT, Long => JLong}


import scala.collection.mutable


class MasterService(val numTask: Int, val syncModel: AsyncModel, val conf: util.Map[String, String])
  extends AngelCleintMasterGrpc.AngelCleintMasterImplBase {
  private val logger = Logger.getLogger(classOf[MasterService].getSimpleName)

  val nextWorkerId = new AtomicInteger(1)
  val nextTaskId = new AtomicInteger(1)

  private var masterId: Long = -1L
  private var masterLocation: Location = _

  private val activeWorker = new ConcurrentHashMap[Long, Location]() // workerId -> location
  private val workerHeartBeat = new ConcurrentHashMap[Long, Long]() // workerId -> timestamp
  private val currentWorkerCmds = new ConcurrentHashMap[Long, Command]() // workerId -> cmd

  private val activeTask = new ConcurrentHashMap[Long, Long]() // taskId -> timestamp
  private val taskInWorker = new ConcurrentHashMap[Long, Long]() // taskId -> workerId
  private val currentClockVector: util.Map[JLong, JINT] = new ConcurrentHashMap[JLong, JINT]() // taskId -> clock
  private val currentBatchSizeVector = new ConcurrentHashMap[Long, Int]() // taskId -> batchSize


  private val timeOut = 120000
  // Executors.newSingleThreadExecutor().execute(new HeartBeat)

  override def registerWorker(request: RegisterWorkerReq, responseObserver: StreamObserver[RegisterWorkerResp]): Unit = synchronized {
    try {
      val location = new Location(request.getHost, request.getPort)
      val exists = activeWorker.containsValue(location)

      if (exists) {
        throw new Exception(s"Worker[${location.getIp}:${location.getPort}] has registered! ")
      } else {
        val workerId = nextWorkerId.getAndIncrement()
        activeWorker.put(workerId, location)
        workerHeartBeat.put(workerId, System.currentTimeMillis())
        currentWorkerCmds.put(workerId, Command.NOTHING)
        val isChief = workerId == 1

        val resp = RegisterWorkerResp.newBuilder()
          .setWorkId(workerId)
          .setIsChief(isChief)
          .setAsyncModel(syncModel)

        if (conf != null) {
          resp.putAllConf(conf)
        }
        logger.info(s"${location.toString} registered!")

        responseObserver.onNext(resp.build())
        responseObserver.onCompleted()
      }
    } catch {
      case e: Exception => responseObserver.onError(e)
      case ae: AssertionError => responseObserver.onError(ae)
    }
  }

  override def registerTask(request: RegisterTaskReq, responseObserver: StreamObserver[RegisterTaskResp]): Unit = synchronized {
    try {
      val workId = request.getWorkId
      assert(activeWorker.containsKey(workId))

      val taskId: Long = nextTaskId.getAndIncrement()
      taskInWorker.put(taskId, workId)
      activeTask.put(taskId, System.currentTimeMillis())
      currentClockVector.put(taskId, 0)

      val resp = RegisterTaskResp.newBuilder()
        .setTaskId(taskId)
        .setNumTask(numTask)
        .putAllClock(currentClockVector)
        .build()

      responseObserver.onNext(resp)
      responseObserver.onCompleted()
    } catch {
      case e: Exception => responseObserver.onError(e)
      case ae: AssertionError => responseObserver.onError(ae)
    }
  }

  override def setAngelLocation(request: SetAngelLocationReq, responseObserver: StreamObserver[VoidResp]): Unit = synchronized {
    logger.info("setAngelLocation")
    try {
      if (masterId != -1 || masterLocation != null) {
        throw new Exception("the master has been set, error!")
      } else {
        masterId = request.getWorkId
        masterLocation = new Location(request.getHost, request.getPort)
        assert(request.getWorkId == 1)

        val resp = VoidResp.newBuilder().setMsg("OK").build()
        responseObserver.onNext(resp)
        responseObserver.onCompleted()
      }
    } catch {
      case e: Exception => responseObserver.onError(e)
      case ae: AssertionError => responseObserver.onError(ae)
    }
  }

  override def getAngelLocation(request: VoidReq, responseObserver: StreamObserver[GetAngelLocationResp]): Unit = synchronized {
    try {
      var time = 0
      while ((masterId == -1 || masterLocation == null) && time < timeOut) {
        Thread.sleep(100)
        time += 100
      }

      if (time >= timeOut) {
        throw new Exception("angel master address is unknown !")
      }

      if (!activeWorker.containsKey(request.getItemId)) {
        throw new Exception("unknown worker!")
      }

      val resp = GetAngelLocationResp.newBuilder()
        .setHost(masterLocation.getIp)
        .setPort(masterLocation.getPort)
        .build()
      responseObserver.onNext(resp)
      responseObserver.onCompleted()
    } catch {
      case e: Exception => responseObserver.onError(e)
    }
  }

  override def heartBeat(request: HeartBeatReq, responseObserver: StreamObserver[HeartBeatResp]): Unit = {
    try {
      val workerId = request.getWorkId
      val cmd = if (activeWorker.containsKey(workerId)) {
        workerHeartBeat.put(workerId, System.currentTimeMillis())

        currentWorkerCmds.get(workerId)
      } else {
        Command.NOTHING
      }


      val resp = HeartBeatResp.newBuilder()
        .setCmd(cmd)
        .build()
      responseObserver.onNext(resp)
      responseObserver.onCompleted()
    } catch {
      case e: Exception => responseObserver.onError(e)
    }
  }

  override def clock(request: ClockReq, responseObserver: StreamObserver[ClockResp]): Unit = {
    try {
      val taskId = request.getTaskId
      assert(activeTask.containsKey(taskId))

      val batchSize = request.getBatchSize
      currentBatchSizeVector.put(taskId, batchSize)

      val taskClock = currentClockVector.get(taskId)
      assert(taskClock <= request.getClock)
      currentClockVector.put(taskId, request.getClock)

      logger.info(s"{taskId: $taskId, lastClock: $taskClock, thisClock: ${request.getClock}, batch: $batchSize}")

      val resp = ClockResp.newBuilder()
        .setTaskId(taskId)
        .putAllClockMap(currentClockVector)
        .build()

      responseObserver.onNext(resp)
      responseObserver.onCompleted()
    } catch {
      case e: Exception => responseObserver.onError(e)
    }
  }

  override def getClockMap(request: GetClockMapReq, responseObserver: StreamObserver[GetClockMapResp]): Unit = {
    try {
      val taskId = request.getTaskId
      assert(activeTask.containsKey(taskId))

      val iter = currentClockVector.entrySet().iterator()
      val list = new mutable.ListBuffer[String]()
      while (iter.hasNext) {
        val entry = iter.next()
        list.append(s" ${entry.getKey}:${entry.getValue} ")
      }
      logger.info(s"getClockMap: $taskId -> " + list.mkString("[", ",", "]"))


      val resp = GetClockMapResp.newBuilder()
        .setTaskId(taskId)
        .putAllClockMap(currentClockVector)
        .build()

      responseObserver.onNext(resp)
      responseObserver.onCompleted()
    } catch {
      case e: Exception => responseObserver.onError(e)
    }
  }

  override def completeTask(request: CompleteTaskReq, responseObserver: StreamObserver[VoidResp]): Unit = synchronized {
    try {
      val taskId = request.getTaskId

      val workerId = taskInWorker.get(taskId)
      if (!taskInWorker.containsValue(workerId)) {
        activeWorker.remove(workerId)
        workerHeartBeat.remove(workerId)
        currentWorkerCmds.remove(workerId)
      }

      taskInWorker.remove(taskId)
      activeTask.remove(taskId)
      currentClockVector.remove(taskId)
      currentBatchSizeVector.remove(taskId)

      val resp = VoidResp.newBuilder().build()
      responseObserver.onNext(resp)
      responseObserver.onCompleted()
    } catch {
      case e: Exception => responseObserver.onError(e)
    }
  }

  override def getGlobalBatchSize(request: VoidReq, responseObserver: StreamObserver[GetGlobalBatchResp]): Unit = {
    try {
      var batchSize = 0
      assert(activeTask.containsKey(request.getItemId) || activeWorker.containsKey(request.getItemId))
      val iter = currentBatchSizeVector.values().iterator()
      while (iter.hasNext) {
        batchSize += iter.next()
      }

      val resp = GetGlobalBatchResp.newBuilder()
        .setBatchSize(batchSize)
        .build()

      responseObserver.onNext(resp)
      responseObserver.onCompleted()
    } catch {
      case e: Exception => responseObserver.onError(e)
    }
  }

  private class HeartBeat extends Runnable {
    override def run(): Unit = {
      while (!Thread.currentThread().isInterrupted) {
        Thread.sleep((timeOut + 2) / 3)

        if (!workerHeartBeat.isEmpty) {
          val iter = workerHeartBeat.entrySet().iterator()
          while (iter.hasNext) {
            val entry = iter.next()
            val workId = entry.getKey
            if (entry.getValue + timeOut > System.currentTimeMillis()) { // timeout
              // remove worker, and task in that worker
              MasterService.this.synchronized {
                val taskIdsNeed2Removed = new mutable.ListBuffer[Long]()
                // 1. remove taskId in taskInWorker and collect taskIds Need to Removed
                val itertw = taskInWorker.entrySet().iterator()
                while (itertw.hasNext) {
                  val twEntry = itertw.next()
                  val taskId = twEntry.getKey
                  if (twEntry.getValue == workId) {
                    taskIdsNeed2Removed.append(taskId)
                    itertw.remove()
                  }
                }

                // 2. remove taskIds if any
                if (taskIdsNeed2Removed.nonEmpty) {
                  taskIdsNeed2Removed.foreach { taskId =>
                    activeTask.remove(taskId)
                    currentClockVector.remove(taskId)
                    currentBatchSizeVector.remove(taskId)
                  }
                }

                // 3. remove workId in activeWorker
                activeWorker.remove(workId)
                currentWorkerCmds.remove(workId)

                // 4. remove workId in workerHeartBeat
                iter.remove()
              }
            }
          }
        }
      }
    }
  }

}
