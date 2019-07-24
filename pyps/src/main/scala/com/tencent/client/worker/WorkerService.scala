package com.tencent.client.worker

import java.util
import java.util.logging.Logger

import com.tencent.client.worker.protos._
import io.grpc.stub.StreamObserver
import java.lang.{Long => JLong}
import java.util.concurrent.{ConcurrentHashMap, Executors}

import com.google.protobuf.ByteString
import com.tencent.client.common._
import com.tencent.client.worker.ps.tensor.Tensor
import com.tencent.client.common.protos.VoidResp
import com.tencent.client.master.protos.Command._
import com.tencent.client.plasma.PlasmaClient
import com.tencent.client.worker.ps.common._
import com.tencent.client.worker.ps.updater.Optimizer
import com.tencent.client.worker.ps.variable._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.util.ShutdownHookManager



@throws[Exception]
class WorkerService(worker: Worker, masterHost: String, masterPort: Int) extends ClientWorkerGrpc.ClientWorkerImplBase {
  private val logger = Logger.getLogger(classOf[Worker].getSimpleName)
  private val taskMap = new ConcurrentHashMap[JLong, Task]()

  logger.info("start to connect to master!")
  private val masterStub = new MStub(masterHost, masterPort)
  logger.info("connected to master!")

  logger.info("start to registerWorker!")
  private val workerInfo = masterStub.registerWorker()
  logger.info(s"{workerId: ${workerInfo.workId}, isChief: ${workerInfo.isChief}, asyncModel: ${workerInfo.asyncModel}}")

  logger.info("begin to init ClientContext")
  val clientContext = new ClientContext(masterStub.workId, workerInfo.conf)
  if (workerInfo.isChief) {
    logger.info("start Angel master")
    clientContext.startAngel()
    logger.info("Angel master started!")
    val masterLocation = clientContext.getMasterLocation
    logger.info(s"Angel master Location : ${masterLocation.toString}")

    logger.info("send Angel Master Location to Client Master!")
    masterStub.setAngelLocation(masterLocation)

    logger.info("start Angel PSAgent!")
    clientContext.startPSAgent()
    logger.info("Angel PSAgent started!")
  } else {
    logger.info("sleep 10 second waiting for Angel Master to start!")
    Thread.sleep(10000)

    logger.info("get location of Angel Master form Client Master!")
    val masterLocation = masterStub.getAngelLocation
    logger.info(s"Angel master Location : ${masterLocation.toString}")

    logger.info("start Angel PSAgent!")
    clientContext.setMasterLocation(masterLocation)
    clientContext.startPSAgent()
    logger.info("Angel PSAgent started!")
  }

  private val masterContext = MasterContext(clientContext.getAngelClient, workerInfo.asyncModel)
  private val workerContext = WorkerContext(clientContext.getPSAgent, workerInfo.asyncModel)

  Executors.newSingleThreadExecutor().execute(new HeartBeat)

  // plasma client
  private val plasmaCmd = workerInfo.conf.get(PlasmaClient.Plasma_Store_Path)
  private val suffix = workerInfo.conf.get(PlasmaClient.Plasma_Store_Suffix)
  private val memoryGB = workerInfo.conf.get(PlasmaClient.Plasma_Store_MemoryGB).toInt
  PlasmaClient.startObjectStore(plasmaCmd, suffix, memoryGB)


  private def getTask(taskId: Long) = {
    if (!taskMap.containsKey(taskId)) {
      val isChief = taskMap.size() == 0 && workerInfo.isChief
      val taskTemp = new Task(masterStub, workerInfo, isChief)

      taskMap.put(taskId, taskTemp)
      logger.info("begin to register task to Client Master")
      taskTemp.register()
      logger.info(s"task info: {taskId: ${taskTemp.taskId}, isChief: $isChief}")
      taskTemp
    } else {
      taskMap.get(taskId)
    }
  }

  private def getInitializer(initializerParams: util.Map[String, String]): Initializer = {
    if (initializerParams.isEmpty) {
      new NormalInitializer(0.0, 1e-6)
    } else {
      val mean = initializerParams.getOrDefault("mean", "0.0").toDouble
      val std = initializerParams.getOrDefault("std", "0.000001").toDouble
      new NormalInitializer(mean, std)
    }
  }

  private def getUpdater(updaterParams: util.Map[String, String]): Updater = {
    Optimizer.get(Utils.toMap(updaterParams))
  }

  override def createTensor(request: RPCTensor, responseObserver: StreamObserver[CreateResp]): Unit = {
    try {
      val taskId = request.getTaskId
      val task = getTask(taskId)

      // Tensor(name: String, dim: Int, shape: Array[Long], dtype: String, validIndexNum: Long,
      //             initializer: Initializer = new NormalInitializer(0.0, 1e-6))
      val initializer: Initializer = getInitializer(request.getInitializerParamsMap)
      val tensor = new Tensor(request.getName, request.getDim, Utils.toArray(request.getShapeList),
        request.getDtype, request.getValidIndexNum, initializer)

      if (task.isChief) {
        tensor.create(masterContext)
      } else {
        tensor.create(workerContext)
      }

      task.put(request.getName, tensor)

      val matId = tensor.getMatClient.getMatrixId
      val createResp = CreateResp.newBuilder()
        .setTaskId(task.taskId)
        .setMatId(matId)
        .build()
      responseObserver.onNext(createResp)
      responseObserver.onCompleted()
    } catch {
      case e: Exception => responseObserver.onError(e)
      case ae: AssertionError => responseObserver.onError(ae)
    }
  }

  override def createVariable(request: RPCVariable, responseObserver: StreamObserver[CreateResp]): Unit = {
    try {
      val taskId = request.getTaskId
      val task = getTask(taskId)

      // Variable(name: String, dim: Int, shape: Array[Long], dtype: String, validIndexNum: Long,
      //          updater: Updater, initializer: Initializer)
      val updater: Updater = getUpdater(request.getUpdaterParamsMap)
      val initializer: Initializer = getInitializer(request.getInitializerParamsMap)
      val variable = new Variable(request.getName, request.getDim, Utils.toArray(request.getShapeList),
        request.getDtype, request.getValidIndexNum, updater, initializer)

      if (task.isChief) {
        variable.create(masterContext)
      } else {
        variable.create(workerContext)
      }

      task.put(request.getName, variable)

      val matId = variable.getMatClient.getMatrixId
      val createResp = CreateResp.newBuilder()
        .setTaskId(task.taskId)
        .setMatId(matId)
        .build()
      responseObserver.onNext(createResp)
      responseObserver.onCompleted()
    } catch {
      case e: Exception => responseObserver.onError(e)
      case ae: AssertionError => responseObserver.onError(ae)
    }
  }

  override def createEmbedding(request: RPCEmbedding, responseObserver: StreamObserver[CreateResp]): Unit = {
    try {
      val taskId = request.getTaskId
      val task = getTask(taskId)

      // Embedding(name: String,
      //                numFeats: Long,
      //                embeddingSize: Int,
      //                dtype: String,
      //                updater: Updater,
      //                initializer: Initializer)
      val updater: Updater = getUpdater(request.getUpdaterParamsMap)
      val initializer: Initializer = getInitializer(request.getInitializerParamsMap)
      val embedding = new Embedding(request.getName, request.getNumFeats, request.getEmbeddingSize,
        request.getDtype, updater, initializer)

      if (task.isChief) {
        embedding.create(masterContext)
      } else {
        embedding.create(workerContext)
      }

      task.put(request.getName, embedding)

      val matId = embedding.getMatClient.getMatrixId
      val createResp = CreateResp.newBuilder()
        .setTaskId(task.taskId)
        .setMatId(matId)
        .build()
      responseObserver.onNext(createResp)
      responseObserver.onCompleted()
    } catch {
      case e: Exception => responseObserver.onError(e)
      case ae: AssertionError => responseObserver.onError(ae)
    }
  }

  override def init(request: TensorLike, responseObserver: StreamObserver[VoidResp]): Unit = {
    try {
      val taskId = request.getTaskId
      val task = getTask(taskId)

      val tsLike = task.get(request.getMatId)

      if (task.isChief) {
        tsLike.init(masterContext)
      } else {
        tsLike.init(workerContext)
      }

      val resp = VoidResp.newBuilder().build()
      responseObserver.onNext(resp)
      responseObserver.onCompleted()
    } catch {
      case e: Exception => responseObserver.onError(e)
      case ae: AssertionError => responseObserver.onError(ae)
    }
  }

  override def load(request: LoadTensorLike, responseObserver: StreamObserver[VoidResp]): Unit = {
    try {
      val taskId = request.getTaskId
      val task = getTask(taskId)

      val tsLike = task.get(request.getMatId)
      val conf: Configuration = new Configuration()
      Utils.fillConf(request.getConfMap, conf)

      if (task.isChief) {
        tsLike.load(masterContext, request.getPath, conf)
      } else {
        tsLike.load(workerContext, request.getPath, conf)
      }

      val resp = VoidResp.newBuilder().build()
      responseObserver.onNext(resp)
      responseObserver.onCompleted()
    } catch {
      case e: Exception => responseObserver.onError(e)
      case ae: AssertionError => responseObserver.onError(ae)
    }
  }

  override def save(request: SaveTensorLike, responseObserver: StreamObserver[VoidResp]): Unit = {
    try {
      val taskId = request.getTaskId
      val task = getTask(taskId)

      val tsLike = task.get(request.getMatId)

      if (task.isChief) {
        tsLike.save(masterContext, request.getPath, request.getFormatClassName)
      } else {
        tsLike.save(workerContext, request.getPath, request.getFormatClassName)
      }

      val resp = VoidResp.newBuilder().build()
      responseObserver.onNext(resp)
      responseObserver.onCompleted()
    } catch {
      case e: Exception => responseObserver.onError(e)
      case ae: AssertionError => responseObserver.onError(ae)
    }
  }

  override def pull(request: PullRequest, responseObserver: StreamObserver[PullResponse]): Unit = {
    var plasma: PlasmaClient = null
    try {
      plasma = PlasmaClient.get
      val taskId = request.getTaskId
      val matId = request.getMatId
      val epoch = request.getEpoch
      val batch = request.getBatch

      val task = getTask(taskId)
      val tsLike = task.get(matId)

      val objectId = request.getObjectId
      val resObjId = PlasmaClient.getObjectId(matId, epoch, batch)
      if (objectId != null) {
        val byteBuf = plasma.getBuffer(objectId.toByteArray, 3000)
        val dataHead = DataHead.fromBuffer(byteBuf)
        val indices = Deserializer.indicesFromBuffer(byteBuf, dataHead, tsLike.getMeta)
        val pulled = tsLike.pull(epoch, Utils.vector2Matrix(indices))
        plasma.put(resObjId, pulled, tsLike.getMeta)
      } else {
        if (!plasma.contains(resObjId)) {
          val pulled = tsLike.pull(epoch, null)
          plasma.put(resObjId, pulled, tsLike.getMeta)
        }
      }

      val resp = PullResponse.newBuilder()
        .setTaskId(taskId)
        .setMatId(matId)
        .setObjectId(ByteString.copyFrom(resObjId))
        .build()

      responseObserver.onNext(resp)
      responseObserver.onCompleted()
    } catch {
      case e: Exception => responseObserver.onError(e)
      case ae: AssertionError => responseObserver.onError(ae)
    } finally {
      if (plasma != null) {
        PlasmaClient.put(plasma)
      }
    }
  }

  override def push(request: PushRequest, responseObserver: StreamObserver[VoidResp]): Unit = {
    var plasma: PlasmaClient = null
    try {
      plasma = PlasmaClient.get
      val taskId = request.getTaskId
      val matId = request.getMatId
      val epoch = request.getEpoch
      val batch = request.getBatch
      val batchSize = request.getBatchSize
      val objectId = request.getObjectId.toByteArray

      val task = getTask(taskId)
      task.epoch = epoch
      task.batch = batch
      task.batchSize = batchSize
      val tsLike = task.get(matId)
      val grad = plasma.getMatrix(objectId, tsLike.getMeta, 30000)
      tsLike.push(grad, 1.0)

      val resp = VoidResp.newBuilder().build()
      responseObserver.onNext(resp)
      responseObserver.onCompleted()
    } catch {
      case e: Exception => responseObserver.onError(e)
      case ae: AssertionError => responseObserver.onError(ae)
    } finally {
      if (plasma != null) {
        PlasmaClient.put(plasma)
      }
    }
  }

  override def release(request: TensorLike, responseObserver: StreamObserver[VoidResp]): Unit = {
    try {
      val taskId = request.getTaskId
      val task = getTask(taskId)
      val matId = request.getMatId
      workerContext.client.releaseMatrix(matId)
      task.remove(matId)

      val resp = VoidResp.newBuilder().build()
      responseObserver.onNext(resp)
      responseObserver.onCompleted()
    } catch {
      case e: Exception => responseObserver.onError(e)
      case ae: AssertionError => responseObserver.onError(ae)
    }
  }

  override def update(request: TensorLike, responseObserver: StreamObserver[VoidResp]): Unit = {
    try {
      val taskId = request.getTaskId
      val task = getTask(taskId)

      val variable = task.getVariable(request.getMatId)
      if (task.isChief) {
        if (!masterContext.isASP) {
          variable.update(masterContext, task.epoch, masterStub.getGlobalBatchSize)
        } else {
          variable.update(masterContext, task.epoch, task.batchSize)
        }
      } else {
        variable.update(workerContext, task.epoch, task.batchSize)
      }


      val resp = VoidResp.newBuilder().build()
      responseObserver.onNext(resp)
      responseObserver.onCompleted()
    } catch {
      case e: Exception => responseObserver.onError(e)
      case ae: AssertionError => responseObserver.onError(ae)
    }
  }

  override def sync(request: SyncRequest, responseObserver: StreamObserver[VoidResp]): Unit = {
    try {
      val taskId = request.getTaskId
      val task = getTask(taskId)

      task.sync()

      val resp = VoidResp.newBuilder().build()
      responseObserver.onNext(resp)
      responseObserver.onCompleted()
    } catch {
      case e: Exception => responseObserver.onError(e)
      case ae: AssertionError => responseObserver.onError(ae)
    }
  }

  private class HeartBeat extends Runnable {
    override def run(): Unit = {
      while (!Thread.currentThread().isInterrupted) {
        val cmd = masterStub.sendHeartBeat

        cmd match {
          case STOPANGEL =>
            clientContext.stopAngel()
            PlasmaClient.killObjectStore()
          case STOPPSAGENT =>
            clientContext.stopPSAgent()
            PlasmaClient.killObjectStore()
          case STOPWORKER =>
            masterStub.shutdown()
            worker.stop()
            PlasmaClient.killObjectStore()
          case NOTHING | UNRECOGNIZED =>
        }

        Thread.sleep(workerInfo.heartBeatInterval)
      }
    }
  }

}
