package com.tencent.client.worker

import java.util
import java.util.logging.Logger

import com.tencent.client.worker.protos._
import io.grpc.stub.StreamObserver
import java.lang.{Long => JLong}
import java.util.concurrent.ConcurrentHashMap

import com.google.protobuf.ByteString
import com.tencent.client.common.{DataHead, Deserializer, Utils}
import com.tencent.client.worker.ps.tensor.Tensor
import com.tencent.client.common.protos.VoidResp
import com.tencent.client.plasma.PlasmaClient
import com.tencent.client.worker.ps.common.{ClientContext, MasterContext, WorkerContext}
import com.tencent.client.worker.ps.updater.Optimizer
import com.tencent.client.worker.ps.variable.{Embedding, Initializer, NormalInitializer, Updater, Variable}
import org.apache.hadoop.conf.Configuration


class WorkerService(masterHost: String, masterPort: Int) extends ClientWorkerGrpc.ClientWorkerImplBase {
  private val logger = Logger.getLogger(classOf[Worker].getSimpleName)
  private val taskMap = new ConcurrentHashMap[JLong, Task]()
  private val plasma = new PlasmaClient("", "", 12)

  private val masterStub = new MStub(masterHost, masterPort)
  masterStub.registerWorker()

  val clientContext = new ClientContext(masterStub.workId, masterStub.conf)
  if (masterStub.isChief) {
    // start angel master
    clientContext.startAngel()
    val masterLocation = clientContext.getMasterLocation
    masterStub.setAngelLocation(masterLocation)
    clientContext.startPSAgent()
  } else {
    Thread.sleep(50000)
    val masterLocation = masterStub.getAngelLocation
    clientContext.setMasterLocation(masterLocation)
    clientContext.startPSAgent()
  }

  private val masterContext = MasterContext(clientContext.getAngelClient, masterStub.asyncModel)
  private val workerContext = WorkerContext(clientContext.getPSAgent, masterStub.asyncModel)

  private def getTask(taskId: Long): Task = {
    if (!taskMap.containsKey(taskId)) {
      val isChief = taskMap.size() == 0 && masterStub.isChief
      val taskTemp = new Task(masterStub, isChief)
      taskMap.put(taskId, taskTemp)
      taskTemp.register()
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

      task.put(request.getName, tensor)

      if (task.isChief) {
        tensor.create(masterContext)
      } else {
        tensor.create(workerContext)
      }

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

      task.put(request.getName, variable)
      if (task.isChief) {
        variable.create(masterContext)
      } else {
        variable.create(workerContext)
      }

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

      task.put(request.getName, embedding)
      if (task.isChief) {
        embedding.create(masterContext)
      } else {
        embedding.create(workerContext)
      }

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
    try {
      val taskId = request.getTaskId
      val matId = request.getMatId
      val epoch = request.getEpoch
      val batch = request.getBatch

      val task = getTask(taskId)
      val tsLike = task.get(matId)

      val objectId = request.getObjectId
      val pulled = if (objectId != null) {
        val byteBuf = plasma.getBuffer(objectId.toByteArray, 3000)
        val dataHead = DataHead.fromBuffer(byteBuf)
        val indices = Deserializer.indicesFromBuffer(byteBuf, dataHead, tsLike.getMeta)
        tsLike.pull(epoch, Utils.vector2Matrix(indices))
      } else {
        tsLike.pull(epoch, null)
      }

      val resObjId = PlasmaClient.getObjectId(taskId, matId, epoch, batch)
      plasma.put(resObjId, pulled, tsLike.getMeta)

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
    }
  }

  override def push(request: PushRequest, responseObserver: StreamObserver[VoidResp]): Unit = {
    try {
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
}
